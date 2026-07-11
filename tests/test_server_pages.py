"""Phase-4 tests: every page serves 200 and wires the shared assets."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from backend import server as srv

PAGES = ["/", "/debrief", "/stats", "/network", "/alerts"]


@pytest.fixture
def client():
    return TestClient(srv.app)


@pytest.mark.parametrize("path", PAGES)
def test_page_serves_and_references_shared_assets(client, path):
    r = client.get(path)
    assert r.status_code == 200
    assert "/assets/style.css" in r.text
    assert "/assets/shared.js" in r.text
    # Shared injected nav placeholder present on every page.
    assert "data-shared-nav" in r.text
    # Viewport meta — required for any mobile layout to apply.
    assert 'name="viewport"' in r.text


def test_style_css_served_with_css_mime(client):
    r = client.get("/assets/style.css")
    assert r.status_code == 200
    assert "text/css" in r.headers["content-type"]
    # Design tokens present.
    assert "--bg:" in r.text


def test_shared_js_served(client):
    r = client.get("/assets/shared.js")
    assert r.status_code == 200
    assert "initNav" in r.text


def test_no_page_hardcodes_its_own_nav_links(client):
    """The nav is injected by shared.js — a page carrying its own literal
    nav links would drift from the shared one."""
    for path in PAGES:
        html = client.get(path).text
        assert '<a href="/debrief">Debrief</a>' not in html, path
