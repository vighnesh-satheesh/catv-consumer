import uuid
import warnings
from enum import Enum

from django.contrib.postgres.fields import ArrayField
from django.db.models import JSONField
from django.contrib.postgres.indexes import GinIndex
from django.db import models
from django.db.models.lookups import IContains
from django.utils.timezone import now
from django_bulk_update.manager import BulkUpdateManager
from enumfields import EnumField

warnings.filterwarnings("once", "This field is deprecated", DeprecationWarning)


class CatvTokens(Enum):
    ETH = 'ETH'
    BTC = 'BTC'
    TRON = 'TRX'
    LTC = 'LTC'
    BCH = 'BCH'
    XRP = 'XRP'
    EOS = 'EOS'
    XLM = 'XLM'
    BNB = 'BNB'
    ADA = 'ADA'
    BSC = 'BSC'
    KLAY = 'KLAY'
    LUNC = 'LUNC'
    FTM = 'FTM'
    POL = 'POL'
    AVAX = 'AVAX'
    DOGE = 'DOGE'
    ZEC = 'ZEC'
    DASH = 'DASH'
    SOL = 'SOL'
    ARB = 'ARB'
    ARBNOVA = 'ARBNOVA'
    OP = 'OP'
    BASE = 'BASE'
    LINEA = 'LINEA'
    BLAST = 'BLAST'
    SCROLL = 'SCROLL'
    MANTLE = 'MANTLE'
    OPBNB = 'OPBNB'
    BTT = 'BTT'
    CELO = 'CELO'
    FRAXTAL = 'FRAXTAL'
    GNOSIS = 'GNOSIS'
    MEMECORE = 'MEMECORE'
    MOONBEAM = 'MOONBEAM'
    MOONRIVER = 'MOONRIVER'
    TAIKO = 'TAIKO'
    XDC = 'XDC'
    APECHAIN = 'APECHAIN'
    WORLD = 'WORLD'
    SONIC = 'SONIC'
    UNICHAIN = 'UNICHAIN'
    ABSTRACT = 'ABSTRACT'
    BERACHAIN = 'BERACHAIN'
    SWELLCHAIN = 'SWELLCHAIN'
    MONAD = 'MONAD'
    HYPEREVM = 'HYPEREVM'
    KATANA = 'KATANA'
    SEI = 'SEI'
    STABLE = 'STABLE'
    PLASMA = 'PLASMA'


class CatvSearchType(Enum):
    PATH = 'path'
    FLOW = 'flow'


class CatvTaskStatusType(Enum):
    PROGRESS = 'progress'
    RELEASED = 'released'
    FAILED = 'failed'


class PostgresILike(IContains):
    lookup_name = 'ilike'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        params = lhs_params + rhs_params
        return '%s ILIKE %s' % (lhs, rhs), params


class PostgresArrayILike(IContains):
    lookup_name = 'arrayilike'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        params = lhs_params + rhs_params
        return 'array_to_text(%s) ILIKE %s' % (lhs, rhs), params


# IndicatorExtraAnnotation
class CustomGinIndex(GinIndex):
    def create_sql(self, model, schema_editor, using=''):
        statement = super().create_sql(model, schema_editor)
        statement.template = "CREATE INDEX %(name)s ON %(table)s%(using)s (%(columns)s gin_trgm_ops)%(extra)s"
        return statement


models.CharField.register_lookup(PostgresILike)
models.TextField.register_lookup(PostgresILike)
ArrayField.register_lookup(PostgresArrayILike)


# models
class BloxyDistribution(models.Model):
    address = models.CharField(null=False, max_length=50)
    depth_limit = models.IntegerField(null=True)
    transaction_limit = models.IntegerField(null=True)
    from_time = models.DateTimeField(null=True)
    till_time = models.DateTimeField(null=True)
    result = JSONField(default=list)
    token_address = models.CharField(null=True, max_length=50)
    updated = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    class Meta:
        db_table = 'api_bloxy_distribution'
        indexes = [
            models.Index(fields=['address', 'depth_limit',
                                 'from_time', 'till_time'])
        ]


class BloxySource(models.Model):
    address = models.CharField(null=False, max_length=50)
    depth_limit = models.IntegerField(null=True)
    transaction_limit = models.IntegerField(null=True)
    from_time = models.DateTimeField(null=True)
    till_time = models.DateTimeField(null=True)
    result = JSONField(default=list)
    token_address = models.CharField(null=True, max_length=50)
    updated = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    class Meta:
        db_table = 'api_bloxy_source'
        indexes = [
            models.Index(fields=['address', 'depth_limit',
                                 'from_time', 'till_time'])
        ]


class CatvRequestStatus(models.Model):
    uid = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    params = JSONField(default=dict)
    status = EnumField(enum=CatvTaskStatusType,
                       default=CatvTaskStatusType.PROGRESS)
    user_id = models.IntegerField(null=False)
    created = models.DateTimeField(default=now)
    updated = models.DateTimeField(auto_now=True)
    labels = ArrayField(models.CharField(max_length=100, blank=False), default=list)
    token_type = EnumField(CatvTokens, default=CatvTokens.ETH)
    is_legacy = models.BooleanField(default=False)
    # parent_request = models.ForeignKey('self', null=True, blank=True,
    #                                    on_delete=models.CASCADE,
    #                                    related_name='swap_requests')
    # is_swap_request = models.BooleanField(default=False)
    is_bounty_track = models.BooleanField(default=False)

    class Meta:
        db_table = 'api_catv_request_status'
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['user_id']),
            models.Index(fields=['uid']),
        ]


# Need RPC
class IndicatorExtraAnnotation(models.Model):
    pattern = models.CharField(max_length=256)
    annotation = models.TextField(blank=True, null=True)
    created = models.DateTimeField(default=now)
    updated = models.DateTimeField(auto_now=True)
    objects = BulkUpdateManager()

    class Meta:
        db_table = 'api_indicator_extra_annotation'
        indexes = [
            CustomGinIndex(fields=['pattern', ]),
            models.Index(fields=['annotation', ]),
            models.Index(fields=['pattern', ]),
        ]


class CatvResult(models.Model):
    request = models.ForeignKey(CatvRequestStatus, null=False,
                                blank=False, on_delete=models.CASCADE, related_name='request')
    result_file_id = models.IntegerField(null=True)

    class Meta:
        db_table = 'api_catv_result'
        indexes = [
            models.Index(fields=['request'])
        ]

class CatvJobQueue(models.Model):
    message = JSONField(default=dict)
    retries_remaining = models.IntegerField(default=3)
    created = models.DateTimeField(default=now)

    class Meta:
        db_table = 'api_catv_job_queue'
        indexes = [
            models.Index(fields=['retries_remaining']),
            models.Index(fields=['created'])
        ]


# New job queue for CATV revamp
class CatvNeoJobQueue(models.Model):
    message = JSONField(default=dict)
    retries_remaining = models.IntegerField(default=3)
    created = models.DateTimeField(default=now)

    class Meta:
        db_table = 'api_neo_catv_job_queue'
        indexes = [
            models.Index(fields=['retries_remaining']),
            models.Index(fields=['created'])
        ]


class CatvCSVJobQueue(models.Model):
    message = JSONField(default=dict)
    retries_remaining = models.IntegerField(default=3)
    created = models.DateTimeField(default=now)

    class Meta:
        db_table = 'api_csv_catv_job_queue'
        indexes = [
            models.Index(fields=['retries_remaining']),
            models.Index(fields=['created'])
        ]


class CatvNeoCSVJobQueue(models.Model):
    message = JSONField(default=dict)
    retries_remaining = models.IntegerField(default=3)
    created = models.DateTimeField(default=now)
    class Meta:
        db_table = 'api_neo_csv_catv_job_queue'
        indexes = [
            models.Index(fields=['retries_remaining']),
            models.Index(fields=['created'])
        ]


class ConsumerErrorLogs(models.Model):
    request = models.ForeignKey(CatvRequestStatus, null=True,
                                blank=False, on_delete=models.CASCADE, related_name='error_logs')
    topic = models.CharField(max_length=100)
    message = JSONField(default=dict)
    error_trace = models.TextField()
    user_error_message = models.TextField(default=None)
    logged_time = models.DateTimeField(default=now)

    class Meta:
        db_table = 'api_consumer_error_logs'
        indexes = [models.Index(fields=["request"])]
