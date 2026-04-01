from .types import ConferenceManifest, ConferencePaper

__all__ = [
    "ConferenceFetchService",
    "ConferenceHTTPClient",
    "ConferenceManifest",
    "ConferencePaper",
    "CookieHeaderSource",
    "_parse_years",
    "derive_pdf_download_referer",
    "main",
]


def __getattr__(name: str):
    if name in {
        "ConferenceFetchService",
        "ConferenceHTTPClient",
        "CookieHeaderSource",
        "_parse_years",
        "derive_pdf_download_referer",
        "main",
    }:
        from .fetch import (
            ConferenceFetchService,
            ConferenceHTTPClient,
            CookieHeaderSource,
            _parse_years,
            derive_pdf_download_referer,
            main,
        )

        exports = {
            "ConferenceFetchService": ConferenceFetchService,
            "ConferenceHTTPClient": ConferenceHTTPClient,
            "CookieHeaderSource": CookieHeaderSource,
            "_parse_years": _parse_years,
            "derive_pdf_download_referer": derive_pdf_download_referer,
            "main": main,
        }
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
