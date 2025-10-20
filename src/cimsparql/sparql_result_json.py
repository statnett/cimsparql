"""Pydantic model for sparql result."""

from __future__ import annotations

from typing import TYPE_CHECKING

from polyfactory.decorators import post_generated
from polyfactory.factories import pydantic_factory
from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from typing import Any, Self


class CimsparqlBaseModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class SparqlResultHead(CimsparqlBaseModel):
    link: list[str] = Field(default_factory=list)
    variables: list[str] = Field(default_factory=list, alias="vars")


class SparqlResultValue(CimsparqlBaseModel):
    value_type: str = Field(alias="type")
    value: str
    datatype: str = ""


class SparqlData(CimsparqlBaseModel):
    bindings: list[dict[str, SparqlResultValue]]

    def values_as_dict(self) -> list[dict[str, str]]:
        return [{k: item.value for k, item in record.items()} for record in self.bindings]


class SparqlResultJson(CimsparqlBaseModel):
    """Data model for rest api response of MIME type.

    application/sparql-result+json

    https://www.w3.org/TR/sparql11-results-json/
    """

    head: SparqlResultHead
    results: SparqlData

    def validate_column_consistency(self) -> Self:
        """Validate column consistency.

        This is an quite expensive validation since it iterates over the entire result.
        Therefore, it is not implemented as a validator, but it must be explicitly called
        when it is desired to perform the validation
        """
        column_set = set(self.head.variables)
        for item in self.results.bindings:
            if set(item.keys()) != column_set:
                raise ValueError(f"Missing variables for {item}. Expected {column_set}")
        return self


class SparqlResultValueFactory(pydantic_factory.ModelFactory[SparqlResultValue]): ...


def build_sparql_result(variables: list[str]) -> SparqlData:
    return SparqlData(
        bindings=[{variable: SparqlResultValueFactory.build() for variable in variables} for _ in range(10)]
    )


class SparqlResultJsonFactory(pydantic_factory.ModelFactory[SparqlResultJson]):
    @post_generated
    @classmethod
    def results(cls, head: SparqlResultHead) -> SparqlData:
        return build_sparql_result(head.variables)

    @classmethod
    def build(cls, factory_use_construct: bool = False, **kwargs: dict[str, Any]) -> SparqlResultJson:
        result: SparqlResultJson = super().build(factory_use_construct=factory_use_construct, **kwargs)
        result.validate_column_consistency()
        return result
