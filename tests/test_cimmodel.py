from mock import Mock

from cimsparql.model import Model


def test_map_data_types(monkeypatch):
    def cim_init(self, *args):
        self._mapper = Mock(have_cim_version=Mock(return_value=True))
        prefix = Mock(prefixes={"cim": None})
        self.client = Mock(prefixes=prefix)

    monkeypatch.setattr(Model, "__init__", cim_init)
    cim_model = Model()
    assert cim_model.map_data_types


def test_not_map_data_types(monkeypatch):
    def cim_init(self, *args):
        self.mapper = Mock(have_cim_version=Mock(return_value=False))
        prefix = Mock(prefixes={"cim": None})
        self.client = Mock(prefixes=prefix)

    monkeypatch.setattr(Model, "__init__", cim_init)
    cim_model = Model()
    assert not cim_model.map_data_types
