import json
import time
import traceback
from operator import gt, lt
from uuid import UUID, uuid4

from django.core.files.base import ContentFile
from django.db import transaction
from django.utils.timezone import now

from api.catvutils.exchange_checker import ExchangeChecker
from api.catvutils.metrics import CatvMetrics
from api.catvutils.smc_method_finder import SmartContractMethodFinder
from api.models import (
    CatvTokens, CatvSearchType,
    CatvRequestStatus, CatvTaskStatusType,
    ConsumerErrorLogs, CatvResult,
    CatvNeoJobQueue
)
from api.rpc.RPCClient import update_s3_attached_file_uid, \
    update_catv_usage_error
from api.serializers import (
    CATVETHSerializer, CATVBTCSerializer,
    CATVBTCPathSerializer, CATVETHPathSerializer
)
from api.settings import api_settings
from api.tasks import catv_history_task, catv_path_history_task

__all__ = ('process_catv_messages',)

from api.utils import upload_content_file_to_gcs, get_file_meta, get_user_error_message, get_gcs_file


def process_catv_messages(job: CatvNeoJobQueue, is_csv_job=False):
    message = job.message
    request_body = message
    print("Processing message:\n")
    print(request_body)

    serializer_map = {
        CatvTokens.ETH.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.BTC.value: {
            CatvSearchType.FLOW.value: CATVBTCSerializer,
            CatvSearchType.PATH.value: CATVBTCPathSerializer
        },
        CatvTokens.TRON.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.LTC.value: {
            CatvSearchType.FLOW.value: CATVBTCSerializer,
            CatvSearchType.PATH.value: CATVBTCPathSerializer
        },
        CatvTokens.BCH.value: {
            CatvSearchType.FLOW.value: CATVBTCSerializer,
            CatvSearchType.PATH.value: CATVBTCPathSerializer
        },
        CatvTokens.XRP.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.EOS.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.XLM.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.BNB.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.ADA.value: {
            CatvSearchType.FLOW.value: CATVBTCSerializer,
            CatvSearchType.PATH.value: CATVBTCPathSerializer
        },
        CatvTokens.BSC.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.KLAY.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.LUNC.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.FTM.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.POL.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.AVAX.value: {
            CatvSearchType.FLOW.value: CATVETHSerializer,
            CatvSearchType.PATH.value: CATVETHPathSerializer
        },
        CatvTokens.DOGE.value: {
            CatvSearchType.FLOW.value: CATVBTCSerializer,
            CatvSearchType.PATH.value: CATVBTCPathSerializer
        },
        CatvTokens.ZEC.value: {
            CatvSearchType.FLOW.value: CATVBTCSerializer,
            CatvSearchType.PATH.value: CATVBTCPathSerializer
        },
        CatvTokens.DASH.value: {
            CatvSearchType.FLOW.value: CATVBTCSerializer,
            CatvSearchType.PATH.value: CATVBTCPathSerializer
        }
    }

    try:
        results = None
        request_instance = None
        request_uid = UUID(request_body["message_id"])
        user_id = request_body["user_id"]
        # parent_result_file_id = request_body.get("parent_result_file_id", None)
        # parent_result = None
        # if parent_result_file_id:
        #     parent_result = get_gcs_file(f'{api_settings.ATTACHED_FILE_S3_KEY_PREFIX + parent_result_file_id}')
        token_type = request_body.get("token_type", CatvTokens.ETH.value)
        search_type = request_body.get("search_type", CatvSearchType.FLOW.value)
        search_params = request_body.get("search_params", {})
        source_depth = search_params.get("source_depth", 0)
        distribution_depth = search_params.get("distribution_depth", 0)
        if source_depth == 0:
            search_params.pop("source_depth", None)
        if distribution_depth == 0:
            search_params.pop("distribution_depth", None)
        search_params.update({'force_lookup': True})
        history_runner = catv_history_task if search_type == CatvSearchType.FLOW.value else catv_path_history_task
        print(search_params)

        serializer_obj = serializer_map[token_type][search_type](data=search_params)
        serializer_obj._token_type = token_type
        serializer_obj.is_valid(raise_exception=True)
        if search_type == CatvSearchType.FLOW.value:
            balanced_tx_limit = search_params['transaction_limit'] if is_csv_job else api_settings.CATV_TX_LIMIT
            balanced_addr_limit = api_settings.CATV_ADDRESS_LIMT
            if source_depth > 0 and distribution_depth > 0:
                balanced_tx_limit = balanced_tx_limit / 2
                balanced_addr_limit = balanced_addr_limit / 2
            core_results = serializer_obj.get_tracking_results(tx_limit=balanced_tx_limit, limit=balanced_addr_limit,
                                                               save_to_db=False)
        else:
            core_results = serializer_obj.get_tracking_results(save_to_db=False)
        graph_data = core_results.get("graph", {})
        if 'graph_node_list' in graph_data and graph_data['graph_node_list']:
            if len(graph_data['node_list']) != len(graph_data['graph_node_list']):
                core_results["messages"][
                    "source"] += f"\nThis address has too many transactions. Viewing all transactions would be difficult, " \
                                 f"so we have generated the most relevant graph for you with some scaling down on each level to show nodes which have transacted the most."
            graph_data["node_list"] = graph_data["graph_node_list"]
            graph_data["edge_list"] = graph_data["graph_edge_list"] if graph_data["graph_edge_list"] else graph_data[
                "edge_list"]
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
            print("Entering SmartContractMethodFinder:")
            start_time = time.time()
            smart_contract_data_obj = SmartContractMethodFinder(token_type, graph_data['node_list'],
                                                                graph_data['edge_list'])
            graph_data["edge_list"] = smart_contract_data_obj.get_updated_edges()
            elapsed_time = time.time() - start_time
            print("Total time taken for SmartContractMethodFinder: ", elapsed_time)

        catv_metrics = CatvMetrics(graph_data, search_params, token_type)
        dist_analysis = {}
        src_analysis = {}
        enhanced_metrics = {}
        if search_type == CatvSearchType.FLOW.value:
            if search_params.get("distribution_depth", 0) > 0:
                results = catv_metrics.generate_metrics(gt)
                dist_analysis = results["legacy_metrics"]
                enhanced_metrics["dist_analysis"] = results["enhanced_metrics"]
            if search_params.get("source_depth", 0) > 0:
                results = catv_metrics.generate_metrics(lt)
                src_analysis = results["legacy_metrics"]
                enhanced_metrics["src_analysis"] = results["enhanced_metrics"]
        else:
            if search_params.get("depth", 0) > 0:
                results = catv_metrics.generate_metrics(gt)
                dist_analysis = results["legacy_metrics"]
                enhanced_metrics["dist_analysis"] = results["enhanced_metrics"]
        graph_data['total_amount'], graph_data['total_amount_usd'] = catv_metrics.calculate_total_amounts()
        catv_metrics.save_annotations()
        print("total number of nodes: ", len(graph_data["node_list"]))
        enhanced_metrics["overview"] = catv_metrics.generate_overview_metrics()
        results = {
            "data": {
                **graph_data,
                "dist_analysis": dist_analysis,
                "src_analysis": src_analysis,
                "metrics": enhanced_metrics
            },
            "messages": {**core_results["messages"]}
        }

        search_params.update({'user_id': user_id, 'token_type': token_type})
        if graph_data.get("node_list", {}):
            history_runner.delay(history=search_params, from_history=False)
            task_status = CatvTaskStatusType.RELEASED
        else:
            res = update_catv_usage_error(user_id)
            print('Node list is empty, setting status as FAILED', res)
            history_runner.delay(history=search_params, from_history=True)
            task_status = CatvTaskStatusType.FAILED
    except Exception as e:
        traceback.print_exc()
        print(f"Inside catvmessages: {type(e)}")
        task_status = CatvTaskStatusType.FAILED
        # revert usage in case of exception
        res = update_catv_usage_error(user_id)
        print('Catv error, updating error usage', res)

        request_instance = CatvRequestStatus.objects.get(uid=request_uid, user_id=user_id)

        # set error log and appropriate error_message
        ConsumerErrorLogs.objects.create(
            topic="catv-requests",
            request=request_instance,
            message=request_body,
            error_trace=traceback.format_exc(),
            user_error_message=get_user_error_message(e)
        )
    finally:
        message = results
        with transaction.atomic():
            attached_file_pk = 0
            # if task_status is failed we don't have to push the response to GCS
            # and make an RPC call to portal to update the AttachedFile uid
            if task_status != CatvTaskStatusType.FAILED:
                file_name = None
                file_uid = uuid4()
                content_file = ContentFile(bytes(json.dumps(message).encode('UTF-8')), name=str(file_uid))
                try:
                    # returns the file name if upload to GCS is successful
                    file_name = upload_content_file_to_gcs(content_file)
                except Exception as e:
                    print("Upload to GCS failed for request_uid: ", request_uid)
                    ConsumerErrorLogs.objects.create(
                        request_uid=request_uid,
                        topic="s3-upload",
                        message=request_body,
                        error_trace=traceback.format_exc(),
                        user_error_message=get_user_error_message(e)
                    )
                if file_name:
                    file_name = f'{file_name}.json'
                    # file meta data
                    hash, size, mimetype = get_file_meta(content_file, file_name)
                    request_dict = {'file_uid': str(file_uid), 'file_name': file_name, 'hash': hash,
                                    'size': size,
                                    'mimetype': str(mimetype)}
                    # rpc to portal for creating AttachedFile table entry
                    #Need to refactor this to be referring to GCS.
                    attached_file_pk = update_s3_attached_file_uid(request_dict)
                    if int(attached_file_pk) == 0:
                        print("AttachedFile uid not updated with S3 file_name through RPC to portal")
                        task_status = CatvTaskStatusType.FAILED
                else:
                    task_status = CatvTaskStatusType.FAILED

            if not request_instance:
                request_instance = CatvRequestStatus.objects.get(uid=request_uid, user_id=user_id)
            request_instance.status = task_status
            request_instance.updated = now()
            request_instance.save()
            if attached_file_pk != 0:
                CatvResult.objects.filter(request=request_instance).update(result_file_id=attached_file_pk)
            job.delete()
