from mock import Mock, patch

from cimsparql.model import Model


@patch.object(Model, "__abstractmethods__", set())
def test_map_data_types(monkeypatch):
    def cim_init(self, *args):
        self._mapper = Mock(have_cim_version=Mock(return_value=True))
        self.prefixes = {"cim": None}

    monkeypatch.setattr(Model, "__init__", cim_init)
    cim_model = Model()
    assert cim_model.map_data_types


@patch.object(Model, "__abstractmethods__", set())
def test_not_map_data_types(monkeypatch):
    def cim_init(self, *args):
        self._mapper = Mock(have_cim_version=Mock(return_value=False))
        self.prefixes = {"cim": None}

    monkeypatch.setattr(Model, "__init__", cim_init)
    cim_model = Model()
    assert not cim_model.map_data_types


@patch.object(Model, "__abstractmethods__", set())
@patch.object(Model, "__init__", Mock(return_value=None))
def test_not_map_data_types_on_exception():
    cim_model = Model(None)
    assert not cim_model.map_data_types
