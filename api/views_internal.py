from rest_framework.views import APIView
from .models import (
    Case,
    Indicator,
    ICO
)
from .serializers import (
    CasePostSerializer,
    IndicatorPostSerializer,
    IndicatorSimpleListSerializer,
    ProjectPostSerializer
)
from .response import APIResponse
from . import permissions


class ProjectInternalView(APIView):
    authentication_classes = ()
    permission_classes = (permissions.InternalOnly,)
    model = ICO

    def post(self, request, format=None):
        serializer = ProjectPostSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        project = serializer.save()
        return APIResponse({
            "data": {
                "project": {
                    "id": project.pk,
                    "uid": project.uid
                }
            }
        })


class CaseIntervalView(APIView):
    authentication_classes = ()
    permission_classes = (permissions.InternalOnly,)
    model = Case

    def post(self, request, format=None):
        serializer = CasePostSerializer(data=request.data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        case = serializer.save()
        return APIResponse({
            "data": {
                "case": {
                    "id": case.pk,
                    "uid": case.uid,
                    "indicators": IndicatorSimpleListSerializer(case.indicators, many=True).data
                }
            }
        })


class IndicatorInternalView(APIView):
    authentication_classes = ()
    permission_classes = (permissions.InternalOnly,)
    model = Indicator

    def post(self, request):
        if "indicators" in request.data:
            serializer = IndicatorPostSerializer(data = request.data["indicators"], many=True)
        else:
            serializer = IndicatorPostSerializer(data = request.data)
        serializer.is_valid(raise_exception=True)
        indicator_obj = serializer.save()
        result_serializer = IndicatorSimpleListSerializer(indicator_obj, many="indicators" in request.data)
        return APIResponse({
            "data": result_serializer.data
        })

