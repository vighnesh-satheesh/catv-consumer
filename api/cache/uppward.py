from django.core.cache import caches, cache
import re
from urllib.parse import urlparse

class UppwardCache:
    def __init__(self):
        pass

    def invalidate_cache(self, u):
        c = caches["uppward"]
        p = re.compile(r"^((https?:\/\/[^\s/$.?#][^\s]*)|((([a-z0-9]|[^\x00-\x7F])([a-z0-9-]|[^\x00-\x7F])*\.)+([a-z]|[^\x00-\x7F])([a-z0-9-]|[^\x00-\x7F]){1,}(:\d{1,5})?(\/.*)?))$", re.IGNORECASE)
        if p.match(u) != None:
            url = urlparse(u).netloc.replace("www.", "").lower()
            d = c.get(url)
            if d is not None:
                c.delete(url)
            d = c.get('www.' + url)
            if d is not None:
                c.delete('www' + url)
