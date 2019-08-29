from typing import Dict


def service(
    server: str = "graphdb.statnett.no", repo: str = "SNMST-Master1Repo-VERSION-LATEST"
) -> str:
    return f"https://{server}/repositories/{repo}"


class Prefix:
    def __init__(self, cim_version: int, prefix_dict: Dict = None):

        self._cim_version = cim_version

        if prefix_dict is None:

            cim_year = {15: 2010, 16: 2013}

            self.prefix_dict = {
                "rdf": "www.w3.org/1999/02/22-rdf-syntax-ns",
                "alg": f"www.alstom.com/grid/CIM-schema-cim{cim_version}-extension",
                "cim": f"iec.ch/TC57/{cim_year[cim_version]}/CIM-schema-cim{cim_version}",
                "SN": f"www.statnett.no/CIM-schema-cim{cim_version}-extension",
                "pti": f"http://www.pti-us.com/PTI_CIM-schema-cim{cim_version}#",
                "md": "http://iec.ch/TC57/61970-552/ModelDescription/1#",
                "entsoe": "http://entsoe.eu/CIM/SchemaExtension/3/1#",
                "entsoe2": "http://entsoe.eu/CIM/SchemaExtension/3/2#",
            }
        else:
            self.prefix_dict = prefix_dict

    def header_str(self) -> str:
        return "\n".join(
            [f"PREFIX {name}:<http://{url}#>" for name, url in self.prefix_dict.items()]
        )

    def ns(self):
        return {name: f"http://{url}#" for name, url in self.prefix_dict.items()}

    def items(self):
        return self.prefix_dict.items()

    def inverse(self):
        return {f"http://{url}#": name for name, url in self.prefix_dict.items()}
