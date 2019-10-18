from mock import MagicMock
from cimsparql.model import CimModel


def test_map_data_types(monkeypatch):
    def cim_init(self, *args):
        self.mapper = MagicMock(have_cim_version=MagicMock(return_value=True))
        self.prefix_dict = {"cim": None}

    monkeypatch.setattr(CimModel, "__init__", cim_init)
    cim_model = CimModel()
    assert cim_model.map_data_types


def test_not_map_data_types(monkeypatch):
    def cim_init(self, *args):
        self.mapper = MagicMock(have_cim_version=MagicMock(return_value=False))
        self.prefix_dict = {"cim": None}

    monkeypatch.setattr(CimModel, "__init__", cim_init)
    cim_model = CimModel()
    assert not cim_model.map_data_types


def test_not_map_data_types_on_exception(monkeypatch):
    def cim_init(self, *args):
        pass

    monkeypatch.setattr(CimModel, "__init__", cim_init)
    cim_model = CimModel()
    assert not cim_model.map_data_types
