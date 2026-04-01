from __future__ import annotations

import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from urllib.error import URLError
from urllib.error import HTTPError
from unittest.mock import patch

from paper_agent.conference.fetch import (
    ConferenceFetchService,
    ConferenceHTTPClient,
    CookieHeaderSource,
    _parse_years,
    derive_pdf_download_referer,
)
from paper_agent.conference.parsing import collect_pdf_candidate_urls, infer_doi_pdf_candidate, parse_html_document
from paper_agent.conference.types import ConferencePaper
from paper_agent.conference.venues.osdi import OSDIAdapter
from paper_agent.conference.venues.popl import POPLAdapter
from paper_agent.conference.venues.pldi import PLDIAdapter
from paper_agent.conference.venues.sosp import SOSPAdapter
from paper_agent.playwright_download import PlaywrightPDFDownloader
from paper_agent.playwright_download import PlaywrightDownloadConfig
from paper_agent.playwright_download import BrowserPDFDownloaderPool
from paper_agent.playwright_download import infer_playwright_browser_fallback_enabled


class DummyClient:
    def __init__(self, pages: dict[str, str]) -> None:
        self.pages = pages

    def fetch_document(self, url: str):
        html = self.pages[url]
        return parse_html_document(html, url=url, final_url=url)


class RecordingBrowserDownloader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Path, str | None]] = []

    def download_pdf(self, url: str, destination: Path, *, referer: str | None = None) -> dict[str, str | int]:
        self.calls.append((url, destination, referer))
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"%PDF-1.7\nstub browser pdf\n")
        return {
            "url": url,
            "final_url": url,
            "destination": str(destination),
            "byte_count": destination.stat().st_size,
            "content_type": "application/pdf",
            "transport": "playwright:stub",
        }


class BlockingBrowserDownloader:
    def __init__(self, transport_label: str, started: Event, release: Event) -> None:
        self.transport_label = transport_label
        self.started = started
        self.release = release

    def download_pdf(self, url: str, destination: Path, *, referer: str | None = None) -> dict[str, str | int]:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"%PDF-1.7\nblocking stub pdf\n")
        self.started.set()
        self.release.wait(timeout=2)
        return {
            "url": url,
            "final_url": url,
            "destination": str(destination),
            "byte_count": destination.stat().st_size,
            "content_type": "application/pdf",
            "transport": self.transport_label,
        }


class FakeLocator:
    def __init__(self, *, visible: bool = False) -> None:
        self.visible = visible
        self.clicked = False

    @property
    def first(self) -> "FakeLocator":
        return self

    def is_visible(self, timeout: int = 0) -> bool:
        return self.visible

    def click(self, timeout: int = 0) -> None:
        self.clicked = True


class FakeFrame:
    def __init__(self, *, url: str = "", name: str = "", locators: dict[str, FakeLocator] | None = None) -> None:
        self.url = url
        self.name = name
        self._locators = locators or {}

    def locator(self, selector: str) -> FakeLocator:
        return self._locators.get(selector, FakeLocator())


class FakePage(FakeFrame):
    def __init__(
        self,
        *,
        url: str,
        title: str = "",
        frames: list[FakeFrame] | None = None,
        locators: dict[str, FakeLocator] | None = None,
    ) -> None:
        super().__init__(url=url, name="page", locators=locators)
        self._title = title
        self.frames = frames or []

    def title(self) -> str:
        return self._title


class ConferenceFetchParsingTests(unittest.TestCase):
    def test_collect_pdf_candidates_prefers_meta_pdf(self) -> None:
        document = parse_html_document(
            """
            <html>
              <head>
                <meta name="citation_pdf_url" content="/papers/test-paper.pdf" />
              </head>
              <body>
                <a href="/download">Download</a>
              </body>
            </html>
            """,
            url="https://example.org/paper",
            final_url="https://example.org/paper",
        )
        candidates = collect_pdf_candidate_urls(document)
        self.assertEqual(candidates[0], "https://example.org/papers/test-paper.pdf")

    def test_osdi_adapter_discovers_presentation_links(self) -> None:
        client = DummyClient(
            {
                "https://www.usenix.org/conference/osdi24/technical-sessions": """
                <html>
                  <body>
                    <a href="/conference/osdi24/presentation/alpha">A Better Systems Paper</a>
                    <a href="/conference/osdi24/presentation/beta">Another Great Result</a>
                    <a href="/conference/osdi24">OSDI 24</a>
                  </body>
                </html>
                """
            }
        )
        adapter = OSDIAdapter()
        _, papers = adapter.discover_papers(2024, client)
        self.assertEqual([paper.title for paper in papers], ["A Better Systems Paper", "Another Great Result"])

    def test_pldi_adapter_enriches_pdf_and_authors(self) -> None:
        client = DummyClient(
            {
                "https://pldi24.sigplan.org/details/pldi-2024-papers/79/test-paper": """
                <html>
                  <head>
                    <meta name="citation_title" content="Hyperblock Scheduling for Verified High-Level Synthesis" />
                    <meta name="citation_author" content="Alice Example" />
                    <meta name="citation_author" content="Bob Example" />
                    <meta name="citation_pdf_url" content="https://example.org/preprint.pdf" />
                  </head>
                  <body></body>
                </html>
                """
            }
        )
        adapter = PLDIAdapter()
        paper = ConferencePaper(
            venue="pldi",
            year=2024,
            title="Placeholder",
            detail_url="https://pldi24.sigplan.org/details/pldi-2024-papers/79/test-paper",
        )
        enriched = adapter.enrich_paper(paper, client)
        self.assertEqual(enriched.title, "Hyperblock Scheduling for Verified High-Level Synthesis")
        self.assertEqual(enriched.authors, ["Alice Example", "Bob Example"])
        self.assertEqual(enriched.pdf_url, "https://example.org/preprint.pdf")

    def test_pldi_adapter_falls_back_to_program_blocks(self) -> None:
        client = DummyClient(
            {
                "https://pldi24.sigplan.org/track/pldi-2024-papers": """
                <html>
                  <body>
                    <div>09:00 - 10:20</div>
                    <div>10:40</div>
                    <div>Talk</div>
                    <div>Hyperblock Scheduling for Verified High-Level Synthesis</div>
                    <div>PLDI Research Papers</div>
                    <div>Alice Example, Bob Example</div>
                    <div><a href="https://doi.org/10.1145/3656422.3659980">DOI</a></div>
                    <div><a href="https://arxiv.org/pdf/2401.02948.pdf">Pre-print</a></div>
                    <div>11:00</div>
                    <div>Compiling Something Interesting</div>
                    <div>PLDI Research Papers</div>
                    <div>Carol Example, Dave Example</div>
                    <div><a href="https://doi.org/10.1145/3656422.3659999">DOI</a></div>
                  </body>
                </html>
                """
            }
        )
        adapter = PLDIAdapter()
        _, papers = adapter.discover_papers(2024, client)
        self.assertEqual(len(papers), 2)
        self.assertEqual(papers[0].title, "Hyperblock Scheduling for Verified High-Level Synthesis")
        self.assertEqual(papers[0].authors, ["Alice Example", "Bob Example"])
        self.assertEqual(papers[0].doi_url, "https://doi.org/10.1145/3656422.3659980")
        self.assertEqual(papers[0].preprint_url, "https://arxiv.org/pdf/2401.02948.pdf")

    def test_pldi_adapter_filters_foreign_bracketed_track_titles(self) -> None:
        client = DummyClient(
            {
                "https://pldi24.sigplan.org/track/pldi-2024-papers": """
                <html>
                  <body>
                    <div>09:00 - 10:20</div>
                    <div>10:40</div>
                    <div>Talk</div>
                    <div>[OOPSLA 2023] Two Birds with One Stone: Boosting Code Generation and Code Search via a Generative Adversarial Network</div>
                    <div>Author One, Author Two</div>
                    <div><a href="https://doi.org/10.1145/1111">DOI</a></div>
                    <div>11:00</div>
                    <div>NetBlocks: Staging Layouts for High-Performance Custom Host Network Stacks</div>
                    <div>Author Three, Author Four</div>
                    <div><a href="https://doi.org/10.1145/2222">DOI</a></div>
                  </body>
                </html>
                """
            }
        )
        adapter = PLDIAdapter()
        _, papers = adapter.discover_papers(2024, client)
        self.assertEqual([paper.title for paper in papers], ["NetBlocks: Staging Layouts for High-Performance Custom Host Network Stacks"])

    def test_popl_adapter_discovers_detail_links(self) -> None:
        client = DummyClient(
            {
                "https://popl24.sigplan.org/track/POPL-2024-popl-research-papers": """
                <html>
                  <body>
                    <a href="https://popl24.sigplan.org/details/POPL-2024-popl-research-papers/12/Implementation-and-Synthesis-of-Math-Library-Functions">Implementation and Synthesis of Math Library Functions</a>
                    <a href="https://popl24.sigplan.org/details/POPL-2024-popl-research-papers/15/(TOPLAS)-A-Journal-First-Paper">(TOPLAS) A Journal First Paper</a>
                  </body>
                </html>
                """
            }
        )
        adapter = POPLAdapter()
        _, papers = adapter.discover_papers(2024, client)
        self.assertEqual([paper.title for paper in papers], ["Implementation and Synthesis of Math Library Functions"])
        self.assertEqual(
            papers[0].detail_url,
            "https://popl24.sigplan.org/details/POPL-2024-popl-research-papers/12/Implementation-and-Synthesis-of-Math-Library-Functions",
        )

    def test_popl_adapter_filters_activity_detail_links(self) -> None:
        client = DummyClient(
            {
                "https://popl25.sigplan.org/track/POPL-2025-popl-research-papers": """
                <html>
                  <body>
                    <a href="https://popl25.sigplan.org/details/POPL-2025-popl-research-papers/1/Coinductive-Proofs-for-Temporal-Hyperliveness">Coinductive Proofs for Temporal Hyperliveness</a>
                    <a href="https://popl25.sigplan.org/details/POPL-2025-popl-research-papers/2/POPL-Networking-Reception">POPL Networking Reception</a>
                    <a href="https://popl25.sigplan.org/details/POPL-2025-popl-research-papers/3/Women-POPL-Dinner">Women @ POPL DinnerCatering at Panzano Restaurant</a>
                    <a href="https://popl25.sigplan.org/details/POPL-2025-popl-research-papers/4/Mentoring-Lunch">Mentoring LunchCatering at Hopscotch</a>
                  </body>
                </html>
                """
            }
        )
        adapter = POPLAdapter()
        _, papers = adapter.discover_papers(2025, client)
        self.assertEqual([paper.title for paper in papers], ["Coinductive Proofs for Temporal Hyperliveness"])

    def test_popl_adapter_filters_src_and_sigplan_activity_links(self) -> None:
        client = DummyClient(
            {
                "https://popl25.sigplan.org/track/POPL-2025-popl-research-papers": """
                <html>
                  <body>
                    <a href="https://popl25.sigplan.org/details/POPL-2025-popl-research-papers/1/Linear-Resources-for-Verified-Compilers">Linear Resources for Verified Compilers</a>
                    <a href="https://popl25.sigplan.org/details/POPL-2025-popl-research-papers/2/SRC-Poster-Session">SRC Poster SessionStudent Research Competition at Four Square Corridor</a>
                    <a href="https://popl25.sigplan.org/details/POPL-2025-popl-research-papers/3/SIGPLAN-EC-Meeting">SIGPLAN EC meeting</a>
                  </body>
                </html>
                """
            }
        )
        adapter = POPLAdapter()
        _, papers = adapter.discover_papers(2025, client)
        self.assertEqual([paper.title for paper in papers], ["Linear Resources for Verified Compilers"])

    def test_popl_adapter_falls_back_to_accepted_section_rows(self) -> None:
        client = DummyClient(
            {
                "https://popl25.sigplan.org/track/POPL-2025-popl-research-papers": """
                <html>
                  <body>
                    <h3>Accepted Papers</h3>
                    <div>Title</div>
                    <div>Effectful Program Equivalence by Guarded Interaction TreesPOPLAlice Example, Bob Example</div>
                    <div><a href="https://doi.org/10.1145/1234">DOI</a></div>
                    <div><a href="https://arxiv.org/pdf/2501.00001.pdf">Pre-print</a></div>
                    <div>(TOPLAS) A Journal First PaperPOPLCarol Example, Dave Example</div>
                    <div><a href="https://doi.org/10.1145/5678">DOI</a></div>
                    <div>POPL 2025 Call for Papers</div>
                  </body>
                </html>
                """
            }
        )
        adapter = POPLAdapter()
        _, papers = adapter.discover_papers(2025, client)
        self.assertEqual(len(papers), 1)
        self.assertEqual(papers[0].title, "Effectful Program Equivalence by Guarded Interaction Trees")
        self.assertEqual(papers[0].authors, ["Alice Example", "Bob Example"])
        self.assertEqual(papers[0].doi_url, "https://doi.org/10.1145/1234")
        self.assertEqual(papers[0].preprint_url, "https://arxiv.org/pdf/2501.00001.pdf")

    def test_popl_adapter_strips_virtual_title_decoration_in_accepted_section(self) -> None:
        client = DummyClient(
            {
                "https://popl23.sigplan.org/track/POPL-2023-popl-research-papers": """
                <html>
                  <body>
                    <h3>Accepted Papers</h3>
                    <div>Title</div>
                    <div>Stratified Commutativity in Verification Algorithms for Concurrent ProgramsVirtualPOPLAlice Example, Bob Example</div>
                    <div><a href="https://doi.org/10.1145/7777">DOI</a></div>
                  </body>
                </html>
                """
            }
        )
        adapter = POPLAdapter()
        _, papers = adapter.discover_papers(2023, client)
        self.assertEqual(len(papers), 1)
        self.assertEqual(papers[0].title, "Stratified Commutativity in Verification Algorithms for Concurrent Programs")

    def test_popl_adapter_enriches_pdf_and_authors(self) -> None:
        client = DummyClient(
            {
                "https://popl25.sigplan.org/details/POPL-2025-popl-research-papers/1/example-paper": """
                <html>
                  <head>
                    <meta name="citation_title" content="Effectful Program Equivalence by Guarded Interaction Trees" />
                    <meta name="citation_author" content="Alice Example" />
                    <meta name="citation_author" content="Bob Example" />
                    <meta name="citation_pdf_url" content="https://example.org/popl25-paper.pdf" />
                  </head>
                  <body>
                    <a href="https://doi.org/10.1145/1234">DOI</a>
                  </body>
                </html>
                """
            }
        )
        adapter = POPLAdapter()
        paper = ConferencePaper(
            venue="popl",
            year=2025,
            title="Placeholder",
            detail_url="https://popl25.sigplan.org/details/POPL-2025-popl-research-papers/1/example-paper",
        )
        enriched = adapter.enrich_paper(paper, client)
        self.assertEqual(enriched.title, "Effectful Program Equivalence by Guarded Interaction Trees")
        self.assertEqual(enriched.authors, ["Alice Example", "Bob Example"])
        self.assertEqual(enriched.pdf_url, "https://example.org/popl25-paper.pdf")
        self.assertEqual(enriched.doi_url, "https://doi.org/10.1145/1234")

    def test_popl_adapter_discovers_realistic_program_blocks(self) -> None:
        client = DummyClient(
            {
                "https://popl24.sigplan.org/track/POPL-2024-popl-research-papers": """
                <html>
                  <body>
                    <div>08:50 - 09:00</div>
                    <div>Welcome from the ChairPOPL at Kelvin Lecture +0min</div>
                    <div>09:00 - 10:00</div>
                    <div>Keynote 1POPL at Kelvin Lecture<br />Chair(s): Derek Dreyer</div>
                    <div>09:00</div>
                    <div>60m</div>
                    <div>Talk</div>
                    <div>A New Perspective on Commutativity in Verification</div>
                    <div>Azadeh Farzan University of Toronto</div>
                    <div>10:30 - 11:50</div>
                    <div>Synthesis 1POPL at Kelvin Lecture<br />Chair(s): Soham Chakraborty</div>
                    <div>10:30</div>
                    <div>20m</div>
                    <div>Implementation and Synthesis of Math Library FunctionsDistinguished Paper</div>
                    <div>Ian Briggs University of Utah, Yash Lad University of Utah, Pavel Panchekha University of Utah</div>
                    <div>10:50</div>
                    <div>Enhanced Enumeration Techniques for Syntax-Guided Synthesis of Bit-Vector Manipulations</div>
                    <div>Yuantian Ding Purdue University, Xiaokang Qiu Purdue University</div>
                    <div>11:10</div>
                    <div>Efficient Bottom-Up Synthesis for Programs with Local Variables</div>
                    <div>Xiang Li University of Michigan, Ann Arbor, Xiangyu Zhou University of Michigan</div>
                    <div>Pre-print</div>
                    <div>11:30</div>
                    <div>Optimal Program Synthesis via Abstract Interpretation</div>
                    <div>Stephen Mell University of Pennsylvania, Steve Zdancewic University of Pennsylvania</div>
                    <div>Types 2POPL at Turing Lecture<br />Chair(s): Someone Else</div>
                    <div>Focusing on Refinement Typing (TOPLAS)Remote</div>
                    <div>Dimitrios Economou Queen's University, Canada, Neel Krishnaswami University of Cambridge</div>
                    <div>DOI File Attached</div>
                    <a href="https://arxiv.org/pdf/2401.00001.pdf">Pre-print</a>
                    <a href="https://doi.org/10.1145/1111">DOI</a>
                    <a href="https://doi.org/10.1145/2222">DOI</a>
                    <a href="https://doi.org/10.1145/3333">DOI</a>
                    <a href="https://doi.org/10.1145/4444">DOI</a>
                    <a href="https://example.org/toplas.pdf">File Attached</a>
                  </body>
                </html>
                """
            }
        )
        adapter = POPLAdapter()
        _, papers = adapter.discover_papers(2024, client)
        self.assertEqual(
            [paper.title for paper in papers],
            [
                "Implementation and Synthesis of Math Library Functions",
                "Enhanced Enumeration Techniques for Syntax-Guided Synthesis of Bit-Vector Manipulations",
                "Efficient Bottom-Up Synthesis for Programs with Local Variables",
                "Optimal Program Synthesis via Abstract Interpretation",
            ],
        )
        self.assertEqual(papers[2].preprint_url, "https://arxiv.org/pdf/2401.00001.pdf")
        self.assertEqual(papers[0].session, "Synthesis 1")

    def test_popl_adapter_filters_old_program_noise_and_deduplicates_accepted_section(self) -> None:
        client = DummyClient(
            {
                "https://popl22.sigplan.org/track/POPL-2022-popl-research-papers": """
                <html>
                  <body>
                    <div>09:00 - 10:20</div>
                    <div>Static AnalysisPOPL at Main Hall<br />Chair(s): Someone</div>
                    <div>09:00</div>
                    <div>20m</div>
                    <div>Better Learning through Programming LanguagesInvited TalkInPerson</div>
                    <div>John Speaker Famous University</div>
                    <div>09:20</div>
                    <div>A Static Semantics for StuffInPerson</div>
                    <div>Alice Example Famous University, Bob Example Great Institute</div>
                    <div><a href="https://doi.org/10.1145/9999">DOI</a></div>
                    <h3>Accepted Papers</h3>
                    <div>Title</div>
                    <div>A Static Semantics for StuffPOPLAlice Example Famous University, Bob Example Great Institute</div>
                    <div><a href="https://doi.org/10.1145/9999">DOI</a></div>
                    <div>POPL 2022 Call for Papers</div>
                  </body>
                </html>
                """
            }
        )
        adapter = POPLAdapter()
        _, papers = adapter.discover_papers(2022, client)
        self.assertEqual(len(papers), 1)
        self.assertEqual(papers[0].title, "A Static Semantics for Stuff")
        self.assertEqual(papers[0].session, "Static Analysis")
        self.assertEqual(papers[0].doi_url, "https://doi.org/10.1145/9999")

    def test_sosp_adapter_extracts_title_author_blocks(self) -> None:
        client = DummyClient(
            {
                "https://sigops.org/s/conferences/sosp/2024/accepted.html": """
                <html>
                  <body>
                    <p><strong>Verus: A Practical Foundation for Systems Verification</strong><br />Alice Example, Bob Example</p>
                    <p><strong>RedShift: A Fast Distributed Runtime</strong><br />Carol Example, Dave Example</p>
                  </body>
                </html>
                """
            }
        )
        adapter = SOSPAdapter()
        _, papers = adapter.discover_papers(2024, client)
        self.assertEqual(len(papers), 2)
        self.assertEqual(papers[0].authors, ["Alice Example", "Bob Example"])

    def test_parse_years_supports_ranges(self) -> None:
        self.assertEqual(_parse_years("2023-2025,2021"), [2021, 2023, 2024, 2025])

    def test_infer_doi_pdf_candidate_maps_acm_doi(self) -> None:
        self.assertEqual(
            infer_doi_pdf_candidate("https://doi.org/10.1145/3656422.3659980"),
            "https://dl.acm.org/doi/pdf/10.1145/3656422.3659980",
        )

    def test_listing_page_is_skipped_as_pdf_candidate(self) -> None:
        service = ConferenceFetchService(
            output_root=Path("/tmp/paper-agent-conference-test"),
            run_dir=Path("/tmp/paper-agent-conference-test/logs"),
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            resolve_workers=1,
            download_workers=1,
            retry_attempts=1,
            retry_backoff_seconds=0,
            skip_existing=True,
            dry_run=True,
            enable_supplemental_lookups=False,
            limit_per_venue=None,
        )
        paper = ConferencePaper(
            venue="sosp",
            year=2024,
            title="Example Paper",
            landing_page_url="https://sigops.org/s/conferences/sosp/2024/accepted.html",
        )
        self.assertIsNone(service._resolve_pdf_url(paper))

    def test_prefilter_existing_papers_skips_enrich_for_existing_destination(self) -> None:
        service = ConferenceFetchService(
            output_root=Path("/tmp/paper-agent-conference-test"),
            run_dir=Path("/tmp/paper-agent-conference-test/logs"),
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            resolve_workers=1,
            download_workers=1,
            retry_attempts=1,
            retry_backoff_seconds=0,
            skip_existing=True,
            dry_run=True,
            enable_supplemental_lookups=False,
            limit_per_venue=None,
        )
        paper_existing = ConferencePaper(venue="popl", year=2024, title="Existing Paper")
        paper_pending = ConferencePaper(venue="popl", year=2024, title="Pending Paper")
        existing_destination = service._destination_for_paper(paper_existing)
        existing_destination.parent.mkdir(parents=True, exist_ok=True)
        existing_destination.write_bytes(b"%PDF-1.7\nexisting pdf\n")

        existing, pending = service._prefilter_existing_papers([paper_existing, paper_pending])

        self.assertEqual([paper.title for paper in existing], ["Existing Paper"])
        self.assertEqual([paper.title for paper in pending], ["Pending Paper"])
        self.assertEqual(existing[0].status, "existing")
        self.assertEqual(existing[0].download_path, str(existing_destination))
        self.assertIn("prefilter:reused_existing_file_before_enrich", existing[0].resolution_trace)

    def test_dblp_ee_sources_are_retained_as_alternate_urls(self) -> None:
        service = ConferenceFetchService(
            output_root=Path("/tmp/paper-agent-conference-test"),
            run_dir=Path("/tmp/paper-agent-conference-test/logs"),
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            resolve_workers=1,
            download_workers=1,
            retry_attempts=1,
            retry_backoff_seconds=0,
            skip_existing=True,
            dry_run=True,
            enable_supplemental_lookups=False,
            limit_per_venue=None,
        )

        class DblpStub:
            def fetch_json(self, url: str):
                return (
                    {
                        "result": {
                            "hits": {
                                "hit": [
                                    {
                                        "info": {
                                            "title": "Example Paper",
                                            "year": "2024",
                                            "url": "https://dblp.org/rec/conf/pldi/example",
                                            "ee": [
                                                "https://example.edu/~author/example-paper",
                                                "https://doi.org/10.1145/1234",
                                            ],
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    url,
                )

        service.http = DblpStub()  # type: ignore[assignment]
        paper = ConferencePaper(venue="pldi", year=2024, title="Example Paper")
        paper = service._supplement_from_dblp(paper)
        self.assertIn("https://example.edu/~author/example-paper", paper.alternate_urls)
        self.assertEqual(paper.doi_url, "https://doi.org/10.1145/1234")

    def test_dblp_can_correct_weaker_existing_doi(self) -> None:
        service = ConferenceFetchService(
            output_root=Path("/tmp/paper-agent-conference-test"),
            run_dir=Path("/tmp/paper-agent-conference-test/logs"),
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            resolve_workers=1,
            download_workers=1,
            retry_attempts=1,
            retry_backoff_seconds=0,
            skip_existing=True,
            dry_run=True,
            enable_supplemental_lookups=False,
            limit_per_venue=None,
        )

        class DblpStub:
            def fetch_json(self, url: str):
                return (
                    {
                        "result": {
                            "hits": {
                                "hit": [
                                    {
                                        "info": {
                                            "title": "Example Paper",
                                            "year": "2024",
                                            "doi": "10.1145/5678",
                                            "url": "https://dblp.org/rec/conf/pldi/example",
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    url,
                )

        service.http = DblpStub()  # type: ignore[assignment]
        paper = ConferencePaper(
            venue="pldi",
            year=2024,
            title="Example Paper",
            doi_url="https://doi.org/10.1145/1234",
        )
        paper = service._supplement_from_dblp(paper)
        self.assertEqual(paper.doi_url, "https://doi.org/10.1145/5678")
        self.assertIn("https://doi.org/10.1145/1234", paper.alternate_urls)

    def test_openalex_oa_locations_promote_preprint_and_alternates(self) -> None:
        service = ConferenceFetchService(
            output_root=Path("/tmp/paper-agent-conference-test"),
            run_dir=Path("/tmp/paper-agent-conference-test/logs"),
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            resolve_workers=1,
            download_workers=1,
            retry_attempts=1,
            retry_backoff_seconds=0,
            skip_existing=True,
            dry_run=True,
            enable_supplemental_lookups=False,
            limit_per_venue=None,
        )

        class OpenAlexStub:
            def fetch_json(self, url: str):
                return (
                    {
                        "results": [
                            {
                                "id": "https://openalex.org/W123",
                                "display_name": "Example Paper",
                                "publication_year": 2024,
                                "best_oa_location": {
                                    "pdf_url": "https://arxiv.org/pdf/2401.00001.pdf",
                                    "landing_page_url": "https://arxiv.org/abs/2401.00001",
                                    "source": {"display_name": "arXiv"},
                                },
                                "locations": [
                                    {
                                        "landing_page_url": "https://example.edu/papers/example-paper",
                                        "pdf_url": "",
                                        "source": {"display_name": "Author Page"},
                                    }
                                ],
                                "open_access": {
                                    "oa_url": "https://example.edu/papers/example-paper.pdf",
                                },
                            }
                        ]
                    },
                    url,
                )

        service.http = OpenAlexStub()  # type: ignore[assignment]
        paper = ConferencePaper(venue="sosp", year=2024, title="Example Paper")
        paper = service._supplement_from_openalex(paper)
        self.assertEqual(paper.preprint_url, "https://arxiv.org/pdf/2401.00001.pdf")
        self.assertIn("https://example.edu/papers/example-paper", paper.alternate_urls)
        self.assertIn("https://example.edu/papers/example-paper.pdf", paper.alternate_urls)

    def test_openalex_rate_limit_enables_circuit_breaker(self) -> None:
        service = ConferenceFetchService(
            output_root=Path("/tmp/paper-agent-conference-test"),
            run_dir=Path("/tmp/paper-agent-conference-test/logs"),
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            resolve_workers=1,
            download_workers=1,
            retry_attempts=1,
            retry_backoff_seconds=0,
            skip_existing=True,
            dry_run=True,
            enable_supplemental_lookups=False,
            limit_per_venue=None,
        )

        class OpenAlexRateLimitStub:
            def __init__(self) -> None:
                self.calls = 0

            def fetch_json(self, url: str):
                self.calls += 1
                raise HTTPError(url, 429, "Too Many Requests", hdrs=None, fp=None)

        stub = OpenAlexRateLimitStub()
        service.http = stub  # type: ignore[assignment]
        first = ConferencePaper(venue="popl", year=2024, title="First Paper")
        second = ConferencePaper(venue="popl", year=2024, title="Second Paper")

        first = service._supplement_from_openalex(first)
        second = service._supplement_from_openalex(second)

        self.assertEqual(stub.calls, 1)
        self.assertIn("supplement:openalex_error=HTTP Error 429: Too Many Requests", first.resolution_trace)
        self.assertIn("supplement:openalex_skipped_rate_limited", second.resolution_trace)

    def test_supplement_skips_openalex_when_doi_is_already_present(self) -> None:
        service = ConferenceFetchService(
            output_root=Path("/tmp/paper-agent-conference-test"),
            run_dir=Path("/tmp/paper-agent-conference-test/logs"),
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            resolve_workers=1,
            download_workers=1,
            retry_attempts=1,
            retry_backoff_seconds=0,
            skip_existing=True,
            dry_run=True,
            enable_supplemental_lookups=False,
            limit_per_venue=None,
        )

        class DblpOnlyStub:
            def __init__(self) -> None:
                self.openalex_called = False

            def fetch_json(self, url: str):
                if "api.openalex.org" in url:
                    self.openalex_called = True
                    raise AssertionError("OpenAlex should be skipped when DOI is already available")
                return (
                    {
                        "result": {
                            "hits": {
                                "hit": [
                                    {
                                        "info": {
                                            "title": "Example Paper",
                                            "year": "2024",
                                            "doi": "10.1145/5678",
                                            "url": "https://dblp.org/rec/conf/popl/example",
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    url,
                )

        stub = DblpOnlyStub()
        service.http = stub  # type: ignore[assignment]
        paper = ConferencePaper(venue="popl", year=2024, title="Example Paper")

        paper = service._supplement_paper(paper)

        self.assertEqual(paper.doi_url, "https://doi.org/10.1145/5678")
        self.assertIn("supplement:external_lookup_skipped_doi_already_present", paper.resolution_trace)
        self.assertFalse(stub.openalex_called)

    def test_cookie_header_source_reads_raw_cookie_file(self) -> None:
        cookie_path = Path("/tmp/paper-agent-cookie-header.txt")
        cookie_path.write_text("session=abc123; cf_clearance=token456\n", encoding="utf-8")
        source = CookieHeaderSource.from_inputs(cookie_file=cookie_path)
        self.assertIsNotNone(source)
        self.assertEqual(
            source.header_for_url("https://dl.acm.org/doi/pdf/10.1145/1234"),  # type: ignore[union-attr]
            "session=abc123; cf_clearance=token456",
        )

    def test_cookie_header_source_reads_netscape_cookie_file(self) -> None:
        cookie_path = Path("/tmp/paper-agent-cookie-jar.txt")
        cookie_path.write_text(
            "# Netscape HTTP Cookie File\n"
            ".dl.acm.org\tTRUE\t/\tFALSE\t2147483647\tcf_clearance\tclearance123\n",
            encoding="utf-8",
        )
        source = CookieHeaderSource.from_inputs(cookie_file=cookie_path)
        self.assertIsNotNone(source)
        self.assertEqual(
            source.header_for_url("https://dl.acm.org/doi/pdf/10.1145/1234"),  # type: ignore[union-attr]
            "cf_clearance=clearance123",
        )

    def test_derive_pdf_download_referer_maps_acm_pdf_to_doi_landing(self) -> None:
        self.assertEqual(
            derive_pdf_download_referer("https://dl.acm.org/doi/pdf/10.1145/3656410"),
            "https://dl.acm.org/doi/10.1145/3656410",
        )

    def test_http_client_applies_acm_cookie_header(self) -> None:
        cookie_source = CookieHeaderSource.from_inputs(cookie_header="cf_clearance=clearance123")
        client = ConferenceHTTPClient(
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            retry_attempts=1,
            retry_backoff_seconds=0,
            acm_cookie_source=cookie_source,
        )
        request = client._build_request(
            "https://dl.acm.org/doi/pdf/10.1145/3656410",
            "application/pdf,*/*;q=0.8",
            referer=derive_pdf_download_referer("https://dl.acm.org/doi/pdf/10.1145/3656410"),
        )
        headers = dict(request.header_items())
        self.assertEqual(headers["Cookie"], "cf_clearance=clearance123")
        self.assertEqual(headers["Referer"], "https://dl.acm.org/doi/10.1145/3656410")

    def test_http_client_throttles_repeat_openalex_requests(self) -> None:
        client = ConferenceHTTPClient(
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            retry_attempts=1,
            retry_backoff_seconds=0,
        )
        client._host_next_allowed_at["api.openalex.org"] = time.monotonic() + 0.01

        with patch("paper_agent.conference.fetch.time.sleep") as mocked_sleep:
            client._throttle_for_host("https://api.openalex.org/works?search=example", stage="text")

        mocked_sleep.assert_called_once()

    def test_http_client_caps_large_retry_after_values(self) -> None:
        client = ConferenceHTTPClient(
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            retry_attempts=1,
            retry_backoff_seconds=0,
        )
        error = HTTPError(
            "https://api.openalex.org/works?search=example",
            429,
            "Too Many Requests",
            hdrs={"Retry-After": "43726"},
            fp=None,
        )

        self.assertEqual(client._retry_after_seconds(error), 30.0)

    def test_http_client_short_circuits_openalex_rate_limit_retries(self) -> None:
        client = ConferenceHTTPClient(
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            retry_attempts=4,
            retry_backoff_seconds=2,
        )
        openalex_error = HTTPError(
            "https://api.openalex.org/works?search=example",
            429,
            "Too Many Requests",
            hdrs=None,
            fp=None,
        )

        with (
            patch("paper_agent.conference.fetch.urlopen", side_effect=openalex_error) as mocked_urlopen,
            patch("paper_agent.conference.fetch.time.sleep") as mocked_sleep,
        ):
            with self.assertRaises(HTTPError):
                client.fetch_text("https://api.openalex.org/works?search=example")

        self.assertEqual(mocked_urlopen.call_count, 1)
        mocked_sleep.assert_not_called()

    def test_http_client_forces_browser_transport_for_acm_pdf(self) -> None:
        browser_downloader = RecordingBrowserDownloader()

        class ACMDirectBrowserClient(ConferenceHTTPClient):
            def _download_pdf_via_http(self, url: str, destination: Path):  # type: ignore[override]
                raise AssertionError("HTTP transport should not be used for ACM PDF URLs")

        client = ACMDirectBrowserClient(
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            retry_attempts=1,
            retry_backoff_seconds=0,
            browser_pdf_downloader=browser_downloader,
        )
        destination = Path("/tmp/paper-agent-browser-direct-acm.pdf")
        if destination.exists():
            destination.unlink()
        result = client.download_pdf("https://dl.acm.org/doi/pdf/10.1145/3656410", destination)
        self.assertEqual(result["transport"], "playwright:stub")
        self.assertTrue(destination.exists())
        self.assertEqual(len(browser_downloader.calls), 1)
        self.assertEqual(
            browser_downloader.calls[0][2],
            "https://dl.acm.org/doi/10.1145/3656410",
        )

    def test_download_paper_falls_back_to_doi_when_primary_pdf_candidate_fails(self) -> None:
        service = ConferenceFetchService(
            output_root=Path("/tmp/paper-agent-conference-test"),
            run_dir=Path("/tmp/paper-agent-conference-test/logs"),
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            resolve_workers=1,
            download_workers=1,
            retry_attempts=1,
            retry_backoff_seconds=0,
            skip_existing=False,
            dry_run=False,
            enable_supplemental_lookups=False,
            limit_per_venue=None,
        )

        class DownloadFallbackStub:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def download_pdf(self, url: str, destination: Path) -> dict[str, str | int]:
                self.calls.append(url)
                if "broken-author-copy.pdf" in url:
                    raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(b"%PDF-1.7\nfallback test pdf\n")
                return {
                    "url": url,
                    "final_url": url,
                    "destination": str(destination),
                    "byte_count": destination.stat().st_size,
                    "content_type": "application/pdf",
                    "transport": "http",
                }

        stub = DownloadFallbackStub()
        service.http = stub  # type: ignore[assignment]
        paper = ConferencePaper(
            venue="popl",
            year=2024,
            title="Fallback Example Paper",
            pdf_url="https://example.org/broken-author-copy.pdf",
            doi_url="https://doi.org/10.1145/1234",
        )

        paper = service._download_paper(paper)

        self.assertEqual(
            stub.calls,
            [
                "https://example.org/broken-author-copy.pdf",
                "https://dl.acm.org/doi/pdf/10.1145/1234",
            ],
        )
        self.assertEqual(paper.status, "downloaded")
        self.assertEqual(paper.download_url, "https://dl.acm.org/doi/pdf/10.1145/1234")
        self.assertEqual(
            paper.metadata["download_failures"],
            [{"url": "https://example.org/broken-author-copy.pdf", "error": "HTTP Error 404: Not Found"}],
        )

    def test_http_client_raises_when_acm_pdf_browser_transport_is_unavailable(self) -> None:
        class ACMNoBrowserClient(ConferenceHTTPClient):
            def _download_pdf_via_http(self, url: str, destination: Path):  # type: ignore[override]
                raise AssertionError("HTTP transport should not be attempted for ACM PDF URLs")

        client = ACMNoBrowserClient(
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            retry_attempts=1,
            retry_backoff_seconds=0,
            browser_pdf_downloader=None,
        )
        with self.assertRaisesRegex(RuntimeError, "ACM PDF URLs require Playwright transport"):
            client.download_pdf("https://dl.acm.org/doi/pdf/10.1145/3656410", Path("/tmp/paper-agent-acm-no-browser.pdf"))

    def test_http_client_does_not_fall_back_to_http_when_acm_browser_transport_fails(self) -> None:
        class BrokenBrowserDownloader:
            def download_pdf(self, url: str, destination: Path, *, referer: str | None = None) -> dict[str, str | int]:
                raise RuntimeError("simulated playwright failure")

        class BrowserFirstClient(ConferenceHTTPClient):
            def _download_pdf_via_http(self, url: str, destination: Path):  # type: ignore[override]
                raise AssertionError("HTTP transport should not be used after ACM Playwright failure")

        client = BrowserFirstClient(
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            retry_attempts=1,
            retry_backoff_seconds=0,
            browser_pdf_downloader=BrokenBrowserDownloader(),
        )
        with self.assertRaisesRegex(RuntimeError, "simulated playwright failure"):
            client.download_pdf("https://dl.acm.org/doi/pdf/10.1145/3656410", Path("/tmp/paper-agent-browser-primary-acm.pdf"))

    def test_http_client_does_not_use_browser_fallback_for_non_acm_403(self) -> None:
        browser_downloader = RecordingBrowserDownloader()

        class GenericBlockedClient(ConferenceHTTPClient):
            def _download_pdf_via_http(self, url: str, destination: Path):  # type: ignore[override]
                raise HTTPError(url, 403, "Forbidden", hdrs=None, fp=None)

        client = GenericBlockedClient(
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            retry_attempts=1,
            retry_backoff_seconds=0,
            browser_pdf_downloader=browser_downloader,
        )
        with self.assertRaises(HTTPError):
            client.download_pdf("https://example.org/paper.pdf", Path("/tmp/paper-agent-browser-fallback-non-acm.pdf"))
        self.assertEqual(browser_downloader.calls, [])

    def test_http_client_uses_browser_fallback_for_acm_dns_error(self) -> None:
        browser_downloader = RecordingBrowserDownloader()

        class ACMDNSErrorClient(ConferenceHTTPClient):
            def _download_pdf_via_http(self, url: str, destination: Path):  # type: ignore[override]
                raise AssertionError("HTTP transport should not be attempted for ACM PDF URLs")

        client = ACMDNSErrorClient(
            timeout_seconds=20,
            html_max_bytes=1000,
            download_max_bytes=1000,
            retry_attempts=1,
            retry_backoff_seconds=0,
            browser_pdf_downloader=browser_downloader,
        )
        destination = Path("/tmp/paper-agent-browser-fallback-acm-dns.pdf")
        if destination.exists():
            destination.unlink()
        result = client.download_pdf("https://dl.acm.org/doi/pdf/10.1145/3656410", destination)
        self.assertEqual(result["transport"], "playwright:stub")
        self.assertTrue(destination.exists())
        self.assertEqual(len(browser_downloader.calls), 1)

    def test_browser_downloader_pool_allows_two_parallel_slots(self) -> None:
        first_started = Event()
        second_started = Event()
        release = Event()
        pool = BrowserPDFDownloaderPool(
            [
                BlockingBrowserDownloader("playwright:slot-1", first_started, release),
                BlockingBrowserDownloader("playwright:slot-2", second_started, release),
            ]
        )
        destination_one = Path("/tmp/paper-agent-browser-pool-1.pdf")
        destination_two = Path("/tmp/paper-agent-browser-pool-2.pdf")
        for path in (destination_one, destination_two):
            if path.exists():
                path.unlink()

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_one = executor.submit(pool.download_pdf, "https://dl.acm.org/doi/pdf/10.1145/1111", destination_one)
            future_two = executor.submit(pool.download_pdf, "https://dl.acm.org/doi/pdf/10.1145/2222", destination_two)
            self.assertTrue(first_started.wait(timeout=1))
            self.assertTrue(second_started.wait(timeout=1))
            release.set()
            result_one = future_one.result(timeout=2)
            result_two = future_two.result(timeout=2)

        self.assertEqual({result_one["transport"], result_two["transport"]}, {"playwright:slot-1", "playwright:slot-2"})

    def test_playwright_bootstrap_prefers_same_host_referer(self) -> None:
        self.assertEqual(
            PlaywrightPDFDownloader._bootstrap_url(
                "https://dl.acm.org/doi/pdf/10.1145/3656410",
                referer="https://dl.acm.org/doi/10.1145/3656410",
            ),
            "https://dl.acm.org/doi/10.1145/3656410",
        )

    def test_playwright_challenge_detection_matches_cloudflare_titles(self) -> None:
        self.assertTrue(
            PlaywrightPDFDownloader._looks_like_browser_challenge(
                "https://dl.acm.org/doi/10.1145/3656410",
                "Just a moment...",
            )
        )
        self.assertFalse(
            PlaywrightPDFDownloader._looks_like_browser_challenge(
                "https://dl.acm.org/doi/10.1145/3656410",
                "Daedalus: Safer Document Parsing",
            )
        )

    def test_playwright_cookie_target_filter_prefers_cookie_related_frames(self) -> None:
        downloader = PlaywrightPDFDownloader(
            config=PlaywrightDownloadConfig(cdp_url="http://127.0.0.1:9222"),
            download_max_bytes=1024,
            user_agent="test-agent",
            accept_language="zh-CN",
        )
        page = FakePage(
            url="https://dl.acm.org/doi/10.1145/3656410",
            frames=[
                FakeFrame(url="https://example.org/embed", name="analytics-frame"),
                FakeFrame(url="https://consent.cookiebot.com/uc.js", name="cookiebot-frame"),
            ],
        )

        targets = downloader._candidate_cookie_targets(page)

        self.assertEqual([label for label, _, _ in targets], ["page", "cookiebot-frame"])

    def test_playwright_cookie_banner_accepts_cookiebot_frame_button(self) -> None:
        downloader = PlaywrightPDFDownloader(
            config=PlaywrightDownloadConfig(cdp_url="http://127.0.0.1:9222"),
            download_max_bytes=1024,
            user_agent="test-agent",
            accept_language="zh-CN",
        )
        accept_locator = FakeLocator(visible=True)
        cookie_frame = FakeFrame(
            url="https://consent.cookiebot.com/dialog",
            name="cookiebot-frame",
            locators={
                "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll": accept_locator,
            },
        )
        page = FakePage(
            url="https://dl.acm.org/doi/10.1145/3656410",
            frames=[cookie_frame],
        )

        accepted = downloader._dismiss_cookie_banner(page, budget_ms=500)

        self.assertTrue(accepted)
        self.assertTrue(accept_locator.clicked)

    def test_playwright_browser_fallback_auto_enables_with_cdp(self) -> None:
        self.assertTrue(
            infer_playwright_browser_fallback_enabled(
                explicit_enabled=None,
                env_enabled=False,
                cdp_url="http://127.0.0.1:9222",
                user_data_dir=None,
            )
        )

    def test_playwright_browser_fallback_explicit_disable_overrides_detected_config(self) -> None:
        self.assertFalse(
            infer_playwright_browser_fallback_enabled(
                explicit_enabled=False,
                env_enabled=True,
                cdp_url="http://127.0.0.1:9222",
                user_data_dir="~/Library/Application Support/Google/Chrome",
            )
        )


if __name__ == "__main__":
    unittest.main()
