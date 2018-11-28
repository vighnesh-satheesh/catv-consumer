from rest_framework.response import Response
from rest_framework.renderers import BaseRenderer

from .settings import api_settings


"""
class ApiResponse(Response):
    def __init__(self, *args, **kwargs):
        super(ApiResponse, self).__init__(*args, **kwargs)
        self.data["apiVersion"] = getattr(settings, "API_VERSION", "1.0")
"""


class APIResponse(Response):
    @property
    def rendered_content(self):
        self.data["apiVersion"] = api_settings.VERSION
        return super(APIResponse, self).rendered_content


class FileResponse(Response):
    def __init__(self, data, filename, *args, **kwargs):
        content_type = kwargs.pop("content_type", "application/octet-stream")
        headers = {
            "Content-Disposition": "attachment; filename={0}".format(filename),
            "X-Content-Type-Options": "nosniff",
            "Content-Length": len(data)
        }

        super(FileResponse, self).__init__(data, content_type=content_type,
                                           headers=headers,
                                           *args,
                                           **kwargs)


class FileRenderer(BaseRenderer):
    media_type = "application/octet-stream"
    charset = None
    render_style = "binary"

    def render(self, data, media_type=None, renderer_context=None):
        return data
