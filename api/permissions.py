from rest_framework import permissions
from .settings import api_settings

class IsOwnerOrReadOnly(permissions.BasePermission):
    """
    Custom permission to only allow owners of an object to edit it.
    """

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS and (request.user and request.user.is_authenticated):
            return True

        # Write permissions are only allowed to the owner of the snippet.
        return obj.owner == request.user


class IsPostOrIsAuthenticated(permissions.BasePermission):

    def has_permission(self, request, view):
        if request.method == "POST":
            return True
        return request.user and request.user.is_authenticated


class InternalOnly(permissions.BasePermission):

    def has_permission(self, request, view):
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')

        if ip.startswith("127") or ip.startswith("10.") or ip.startswith("172.") or ip.startswith("192.") or ip.startswith("255."):
            return True
        return False
