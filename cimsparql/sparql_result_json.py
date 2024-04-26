from __future__ import annotations

from typing import TYPE_CHECKING

from polyfactory.decorators import post_generated
from polyfactory.factories import pydantic_factory
from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from typing import Self


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


class SparqlResultJson(CimsparqlBaseModel):
    """
    Data model for rest api resonse of MIME type

    application/sparql-result+json

    https://www.w3.org/TR/sparql11-results-json/
    """

    head: SparqlResultHead
    results: SparqlData

    def validate_column_consistency(self) -> Self:
        """
        This is an quite expensive validation since it iterates over the entire result.
        Therefore, it is not implemented as a validator, but it must be explicitly called
        when it is desired to perform the validation
        """
        column_set = set(self.head.variables)
        for item in self.results.bindings:
            if set(item.keys()) != column_set:
                raise ValueError(f"Missing variables for {item}. Expected {column_set}")
        return self


class SparqlResultValueFactory(pydantic_factory.ModelFactory):
    __model__ = SparqlResultValue


def build_sparql_result(variables: list[str]) -> SparqlData:
    return SparqlData(
        bindings=[
            {variable: SparqlResultValueFactory.build() for variable in variables}
            for _ in range(10)
        ]
    )


class SparqlResultJsonFactory(pydantic_factory.ModelFactory):
    __model__ = SparqlResultJson

    @post_generated
    @classmethod
    def results(cls, head: SparqlResultHead) -> SparqlData:
        return build_sparql_result(head.variables)

    @classmethod
    def build(cls) -> SparqlResultJson:
        result: SparqlResultJson = super().build()
        result.validate_column_consistency()
        return result
