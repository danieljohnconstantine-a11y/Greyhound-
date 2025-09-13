from curl_cffi import requests as cffi_requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .utils import ua_string, env_proxy

HEADERS_BASE = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-AU,en;q=0.9",
    "upgrade-insecure-requests": "1",
}

def _session():
    s = cffi_requests.Session()
    s.headers.update(HEADERS_BASE | {"user-agent": ua_string()})
    proxy = env_proxy()
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    return s

@retry(
    retry=retry_if_exception_type(Exception),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=10),
)
def fetch(url: str, impersonate: str = "chrome120") -> cffi_requests.Response:
    s = _session()
    r = s.get(url, timeout=30, impersonate=impersonate)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r
