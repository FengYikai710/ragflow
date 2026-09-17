#
#  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import sys
import types
import warnings

import pytest

# xgboost imports pkg_resources and emits a deprecation warning that is promoted
# to error in our pytest configuration; ignore it for this unit test module.
warnings.filterwarnings(
    "ignore",
    message="pkg_resources is deprecated as an API.*",
    category=UserWarning,
)


def _install_cv2_stub_if_unavailable():
    try:
        import cv2  # noqa: F401
        return
    except Exception:
        pass

    stub = types.ModuleType("cv2")
    stub.INTER_LINEAR = 1
    stub.INTER_CUBIC = 2
    stub.BORDER_CONSTANT = 0
    stub.BORDER_REPLICATE = 1
    stub.COLOR_BGR2RGB = 0
    stub.COLOR_BGR2GRAY = 1
    stub.COLOR_GRAY2BGR = 2
    stub.IMWRITE_JPEG_QUALITY = 95

    def _missing(*_args, **_kwargs):
        raise RuntimeError("cv2 runtime call is unavailable in this test environment")

    def _module_getattr(name):
        if name.isupper():
            return 0
        return _missing

    stub.__getattr__ = _module_getattr
    sys.modules["cv2"] = stub


_install_cv2_stub_if_unavailable()

from api.db.services.document_service import DocumentService  # noqa: E402


# ---------------------------------------------------------------------------
# Fake ORM helpers – mimic the minimal peewee query chain used by the counter
# ---------------------------------------------------------------------------

class _FakeField:
    """Stand-in for a peewee field supporting `field + 1` inside update()."""

    def __add__(self, other):
        return ("increment", other)


class _FakeQuery:
    """Chains .where()/.execute() without touching a real database."""

    def __init__(self, update_kwargs):
        self.update_kwargs = update_kwargs
        self.where_args = None

    def where(self, *args):
        self.where_args = args
        return self

    def execute(self):
        return 7


def _make_fake_model():
    calls = {}

    class _FakeIdField:
        def in_(self, values):
            calls["in_values"] = list(values)
            return "id_in_expr"

    class _FakeModel:
        retrieval_count = _FakeField()
        id = _FakeIdField()

        @classmethod
        def update(cls, **kwargs):
            calls["update_kwargs"] = kwargs
            return _FakeQuery(kwargs)

    return _FakeModel, calls


@pytest.fixture()
def fake_model(monkeypatch):
    model, calls = _make_fake_model()
    monkeypatch.setattr(DocumentService, "model", model)
    return calls


def _unwrapped_increment():
    """Bypass @classmethod + @DB.connection_context() on increment_retrieval_count."""
    return DocumentService.increment_retrieval_count.__func__.__wrapped__


# ---------------------------------------------------------------------------
# track_retrieval_count – dedupe/filter logic on the final chunks
# ---------------------------------------------------------------------------

class TestTrackRetrievalCount:
    def test_dedupes_doc_ids_across_chunks(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            DocumentService,
            "increment_retrieval_count",
            classmethod(lambda cls, doc_ids: captured.setdefault("doc_ids", doc_ids)),
        )
        chunks = [{"doc_id": "doc-a"}, {"doc_id": "doc-a"}, {"doc_id": "doc-b"}, {"doc_id": "doc-a"}]
        DocumentService.track_retrieval_count(chunks)
        assert set(captured["doc_ids"]) == {"doc-a", "doc-b"}

    def test_filters_empty_and_missing_doc_ids(self, monkeypatch):
        called = []
        monkeypatch.setattr(
            DocumentService,
            "increment_retrieval_count",
            classmethod(lambda cls, doc_ids: called.append(doc_ids)),
        )
        # KG-aggregated chunks carry an empty doc_id; some chunks may miss the key.
        chunks = [{"doc_id": ""}, {"doc_id": None}, {}, {"doc_id": "doc-a"}]
        DocumentService.track_retrieval_count(chunks)
        assert len(called) == 1
        assert called[0] == ["doc-a"]

    def test_noop_when_no_valid_doc_ids(self, monkeypatch):
        called = []
        monkeypatch.setattr(
            DocumentService,
            "increment_retrieval_count",
            classmethod(lambda cls, doc_ids: called.append(doc_ids)),
        )
        DocumentService.track_retrieval_count([])
        DocumentService.track_retrieval_count([{"doc_id": ""}, {}])
        DocumentService.track_retrieval_count(None)
        assert called == []


# ---------------------------------------------------------------------------
# increment_retrieval_count – single atomic UPDATE, never raises
# ---------------------------------------------------------------------------

class TestIncrementRetrievalCount:
    def test_single_update_deduped_by_primary_key(self, fake_model):
        result = _unwrapped_increment()(DocumentService, ["doc-a", "doc-a", "doc-b"])
        assert result == 7
        # update_time/update_date are injected by the real DataBaseModel._normalize_data,
        # not by this method, so the ORM layer only receives the counter itself.
        assert set(fake_model["update_kwargs"]) == {"retrieval_count"}
        assert fake_model["update_kwargs"]["retrieval_count"] == ("increment", 1)
        assert set(fake_model["in_values"]) == {"doc-a", "doc-b"}

    def test_returns_zero_on_empty_input(self, fake_model):
        assert _unwrapped_increment()(DocumentService, []) == 0
        assert _unwrapped_increment()(DocumentService, ["", None]) == 0
        assert "update_kwargs" not in fake_model

    def test_swallows_db_errors(self, monkeypatch):
        class _BrokenModel:
            @classmethod
            def update(cls, **kwargs):
                raise RuntimeError("db down")

        monkeypatch.setattr(DocumentService, "model", _BrokenModel)
        assert _unwrapped_increment()(DocumentService, ["doc-a"]) == 0
