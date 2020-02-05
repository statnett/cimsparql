from mock import MagicMock

from cimsparql.model import CimModel


def test_map_data_types(monkeypatch):
    def cim_init(self, *args):
        self._mapper = MagicMock(have_cim_version=MagicMock(return_value=True))
        self._prefixes = {"cim": None}

    monkeypatch.setattr(CimModel, "__init__", cim_init)
    cim_model = CimModel()
    assert cim_model._map_data_types


def test_not_map_data_types(monkeypatch):
    def cim_init(self, *args):
        self._mapper = MagicMock(have_cim_version=MagicMock(return_value=False))
        self._prefixes = {"cim": None}

    monkeypatch.setattr(CimModel, "__init__", cim_init)
    cim_model = CimModel()
    assert not cim_model._map_data_types


def test_not_map_data_types_on_exception(monkeypatch):
    def cim_init(self, *args):
        pass

    monkeypatch.setattr(CimModel, "__init__", cim_init)
    cim_model = CimModel()
    assert not cim_model._map_data_types
