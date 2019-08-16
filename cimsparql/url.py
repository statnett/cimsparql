def service(
    server: str = "graphdb.statnett.no", repo: str = "SNMST-Master1Repo-VERSION-LATEST"
) -> str:
    return f"https://{server}/repositories/{repo}"


class prefix:
    def __init__(self, cim_version, prefix_dict=None):

        if prefix_dict is None:

            cim_year = {15: 2010, 16: 2013}

            self.prefix_dict = {
                "rdf": "www.w3.org/1999/02/22-rdf-syntax-ns",
                "alg": f"www.alstom.com/grid/CIM-schema-cim{cim_version}-extension",
                "cim": f"iec.ch/TC57/{cim_year[cim_version]}/CIM-schema-cim{cim_version}",
                "SN": f"www.statnett.no/CIM-schema-cim{cim_version}-extension",
            }
        else:
            self.prefix_dict = prefix_dict

    def __str__(self):
        return "\n".join(
            [f"PREFIX {name}:<http://{url}#>" for name, url in self.prefix_dict.items()]
        )

    def ns(self):
        return {name: f"http://{url}#" for name, url in self.prefix_dict.items()}

    def items(self):
        return self.prefix_dict.items()

    def inverse(self):
        return {f"http://{url}#": name for name, url in self.prefix_dict.items()}
