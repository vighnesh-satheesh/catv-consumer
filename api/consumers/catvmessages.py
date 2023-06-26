import json
import traceback
from operator import gt, lt
from uuid import UUID, uuid4

from django.core.files.base import ContentFile
from django.db import transaction
from django.utils.timezone import now
from django.core.files.storage import default_storage

from api.catvutils.exchange_checker import ExchangeChecker
from api.catvutils.metrics import CatvMetrics
from api.catvutils.smc_method_finder import SmartContractMethodFinder
from api.exceptions import FileNotFound
from api.models import (
    CatvTokens, CatvSearchType,
    CatvRequestStatus, CatvTaskStatusType,
    ConsumerErrorLogs, CatvResult,
    CatvJobQueue
)
from api.serializers import (
    CATVSerializer, CATVBTCCoinpathSerializer,
    CatvBtcPathSerializer, CATVEthPathSerializer
)

from api.settings import api_settings
from api.tasks import catv_history_task, catv_path_history_task
from api.rpc.RPCClient import update_s3_attached_file_uid, \
    update_catv_usage_error

__all__ = ('process_catv_messages',)

from api.utils import upload_content_file_to_s3, get_file_meta


def process_catv_messages(job: CatvJobQueue):
    message = job.message
    request_body = message
    print("Processing message:\n")
    print(request_body)

    serializer_map = {
        CatvTokens.ETH.value: {
            CatvSearchType.FLOW.value: CATVSerializer,
            CatvSearchType.PATH.value: CATVEthPathSerializer
        },
        CatvTokens.BTC.value: {
            CatvSearchType.FLOW.value: CATVBTCCoinpathSerializer,
            CatvSearchType.PATH.value: CatvBtcPathSerializer
        },
        CatvTokens.TRON.value: {
            CatvSearchType.FLOW.value: CATVSerializer,
            CatvSearchType.PATH.value: CATVEthPathSerializer
        },
        CatvTokens.LTC.value: {
            CatvSearchType.FLOW.value: CATVBTCCoinpathSerializer,
            CatvSearchType.PATH.value: CatvBtcPathSerializer
        },
        CatvTokens.BCH.value: {
            CatvSearchType.FLOW.value: CATVBTCCoinpathSerializer,
            CatvSearchType.PATH.value: CatvBtcPathSerializer
        },
        CatvTokens.XRP.value: {
            CatvSearchType.FLOW.value: CATVSerializer,
            CatvSearchType.PATH.value: CATVEthPathSerializer
        },
        CatvTokens.EOS.value: {
            CatvSearchType.FLOW.value: CATVSerializer,
            CatvSearchType.PATH.value: CATVEthPathSerializer
        },
        CatvTokens.XLM.value: {
            CatvSearchType.FLOW.value: CATVSerializer,
            CatvSearchType.PATH.value: CATVEthPathSerializer
        },
        CatvTokens.BNB.value: {
            CatvSearchType.FLOW.value: CATVSerializer,
            CatvSearchType.PATH.value: CATVEthPathSerializer
        },
        CatvTokens.ADA.value: {
            CatvSearchType.FLOW.value: CATVBTCCoinpathSerializer,
            CatvSearchType.PATH.value: CatvBtcPathSerializer
        },
        CatvTokens.BSC.value: {
            CatvSearchType.FLOW.value: CATVSerializer,
            CatvSearchType.PATH.value: CATVEthPathSerializer
        },
        CatvTokens.KLAY.value: {
            CatvSearchType.FLOW.value: CATVSerializer,
            CatvSearchType.PATH.value: CATVEthPathSerializer
        },
        CatvTokens.LUNC.value: {
            CatvSearchType.FLOW.value: CATVSerializer,
            CatvSearchType.PATH.value: CATVEthPathSerializer
        }
    }

    try:
        results = None
        message_id = UUID(request_body["message_id"])
        user_id = request_body["user_id"]
        requester = request_body.get("requester", "catv")
        token_type = request_body.get("token_type", CatvTokens.ETH.value)
        search_type = request_body.get("search_type", CatvSearchType.FLOW.value)
        search_params = request_body.get("search_params", {})
        source_depth = search_params.get("source_depth", 0)
        distribution_depth = search_params.get("distribution_depth", 0)
        search_params.update({'force_lookup': True})
        history_runner = catv_history_task if search_type == CatvSearchType.FLOW.value else catv_path_history_task
        print(search_params)
        
        serializer_obj = serializer_map[token_type][search_type](data=search_params)
        serializer_obj._token_type = token_type
        serializer_obj.is_valid(raise_exception=True)
        if search_type == CatvSearchType.FLOW.value:
            balanced_tx_limit = api_settings.CATV_TX_LIMIT
            balanced_addr_limit = api_settings.CATV_ADDRESS_LIMT
            if source_depth > 0 and distribution_depth > 0:
                balanced_tx_limit = balanced_tx_limit / 2
                balanced_addr_limit = balanced_addr_limit / 2
            core_results = serializer_obj.get_tracking_results(tx_limit=balanced_tx_limit, limit=balanced_addr_limit, save_to_db=False)
        else:
            core_results = serializer_obj.get_tracking_results(save_to_db=False)
        graph_data = core_results.get("graph", {})

        if 'graph_node_list' in graph_data and graph_data['graph_node_list']:
            if len(graph_data['node_list']) != len(graph_data['graph_node_list']):
                core_results["messages"]["source"] += f"\nThis address has too many transactions. Viewing all transactions would be difficult, "\
                    f"so we have generated the most relevant graph for you with some scaling down on each level to show nodes which have transacted the most."
            graph_data["node_list"] = graph_data["graph_node_list"]
            graph_data["edge_list"] = graph_data["graph_edge_list"] if graph_data["graph_edge_list"] else graph_data["edge_list"]
            del graph_data["graph_node_list"]
            del graph_data["graph_edge_list"]

        if "exchange" not in graph_data["node_list"][0]["group"].lower():
            print("Origin node is not an exchange")
            exchange_checker_obj = ExchangeChecker(
                source_depth,
                distribution_depth,
                token_type,
                graph_data,
            )
            graph_data = exchange_checker_obj.stop_transfers_at_exchange()

        if token_type in [CatvTokens.ETH.value, CatvTokens.KLAY.value, CatvTokens.BSC.value]:
            smart_contract_data_obj = SmartContractMethodFinder(token_type, graph_data['node_list'], graph_data['edge_list'])
            graph_data["edge_list"] = smart_contract_data_obj.get_updated_edges()

        catv_metrics = CatvMetrics(graph_data)
        dist_analysis = {}
        src_analysis = {}
        if search_type == CatvSearchType.FLOW.value:
            if search_params.get("distribution_depth", 0) > 0:
                dist_analysis = catv_metrics.generate_metrics(gt)
            if search_params.get("source_depth", 0) > 0:
                src_analysis = catv_metrics.generate_metrics(lt)
        else:
            if search_params.get("depth", 0) > 0:
                dist_analysis = catv_metrics.generate_metrics(gt)
        catv_metrics.save_annotations()
        print(len(graph_data["node_list"]))

        results = {
            "data": {
                **graph_data,
                "dist_analysis": dist_analysis,
                "src_analysis": src_analysis
            },
            "messages": {**core_results["messages"]}
        }
        
        search_params.update({'user_id': user_id, 'token_type': token_type})
        if graph_data.get("node_list", {}):
            history_runner.delay(history=search_params, from_history=False)
            task_status = CatvTaskStatusType.RELEASED
        else:
            res = update_catv_usage_error(user_id)
            print('Catv error, updating error usage', res)
            history_runner.delay(history=search_params, from_history=True)
            task_status = CatvTaskStatusType.FAILED
    except Exception as e:
        error_trace = str(e)
        traceback.print_exc()
        generic_error = "Internal server error. Please try again later"
        safe_error_trace = error_trace if isinstance(e, FileNotFound) else generic_error
        error_dict = {
            "data": {},
            "messages": {
                "source": safe_error_trace
            }
        }
        res = update_catv_usage_error(user_id)
        print('Catv error, updating error usage', res)
        task_status = CatvTaskStatusType.FAILED
        ConsumerErrorLogs.objects.create(
            topic="catv-requests",
            message=request_body,
            error_trace=error_trace
        )
    finally:
        message = results or error_dict
        with transaction.atomic():
            attached_file_pk = 0
            # if task_status is failed we don't have to push the response to S3
            # and make an RPC call to portal to update the AttachedFile uid
            if task_status != CatvTaskStatusType.FAILED:
                file_name = None
                file_uid = uuid4()
                content_file = ContentFile(bytes(json.dumps(message).encode('UTF-8')), name=str(file_uid))
                try:
                    # returns the file name if upload to s3 is successful
                    file_name = upload_content_file_to_s3(content_file)
                except Exception:
                    print("Upload to S3 failed: ")
                    traceback.print_exc()
                if file_name:
                    hash, size, mimetype = get_file_meta(content_file.file)
                    request_dict = {'file_uid': str(file_uid), 'file_name': f'{file_name}.json', 'hash': hash,
                                    'size': size,
                                    'mimetype': str(mimetype)}
                    attached_file_pk = update_s3_attached_file_uid(request_dict)
                    if int(attached_file_pk) == 0:
                        print("AttachedFile uid not updated with S3 file_name through RPC to portal")
                        task_status = CatvTaskStatusType.FAILED
                else:
                    task_status = CatvTaskStatusType.FAILED

            request_instance = CatvRequestStatus.objects.get(uid=message_id, user_id=user_id)
            request_instance.status = task_status
            request_instance.updated = now()
            request_instance.save()
            if attached_file_pk != 0:
                CatvResult.objects.filter(request=request_instance).update(result_file_id=attached_file_pk)
            job.delete()
