import argparse
import glob

try:
    from lxml import etree
except ImportError as exc:
    msg = "Require lxml. Install cimsparql with poetry install -E parse_xml"
    raise ImportError(msg) from exc


XML_SCHEMA = "http://www.w3.org/2001/XMLSchema#"
CIM_URI = "http://iec.ch/TC57/2013/CIM-schema-cim16"

REQUIRED_RDF_DATATYPES = {"cim:TransformerEnd.endNumber": f"{XML_SCHEMA}integer"}


def add_required_rdf_dtypes(root: etree.Element):
    if "rdf" not in root.nsmap:
        # Nothing to do if RDF is not in namespace mapping
        return
    rdf_ns = root.nsmap["rdf"]
    for element_name, dtype in REQUIRED_RDF_DATATYPES.items():
        counter = 0
        for element in root.findall(".//" + element_name, root.nsmap):
            element.attrib[f"{{{rdf_ns}}}datatype"] = dtype
            counter += 1

        if counter > 0:
            print(f"Added rdf:datatype type to {counter} occurences of {element_name}")


def main():
    parser = argparse.ArgumentParser(
        "Program that modifies XML files to be compatible with cimsparql"
    )
    parser.add_argument("file", help="File or glob pattern for files to modify")

    base_uri_help = "Base URI to insert in all XML files. For example: " f"{CIM_URI}"
    parser.add_argument("--baseURI", default=CIM_URI, help=base_uri_help)

    suffix_help = (
        "Suffix to the filename after modifying them. If given as an empty "
        "string the original files will be overwritten. Default 'mod'"
    )
    parser.add_argument("--suffix", default="mod", help=suffix_help)
    args = parser.parse_args()

    for f in glob.glob(args.file):
        if f.endswith("xml"):
            print(f"Updating {f}")
            tree = etree.parse(f)  # nosec
            root = tree.getroot()
            root.base = args.baseURI
            add_required_rdf_dtypes(root)

            out_file = f
            if args.suffix:
                out_file = out_file.split(".")[0] + f"{args.suffix}.xml"
            tree.write(out_file)


main()
