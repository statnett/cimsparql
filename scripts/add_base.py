import argparse
import glob
import xml.etree.ElementTree as et


def main():
    parser = argparse.ArgumentParser(
        "Program that inserts a xml:base attribute into the main rdf:RDF element"
    )
    parser.add_argument("file", help="File or glob pattern for files to modify")

    base_uri_help = (
        "Base URI to insert in all XML files. For example: "
        "http://iec.ch/TC57/2013/CIM-schema-cim16"
    )
    parser.add_argument("baseURI", help=base_uri_help)

    suffix_help = (
        "Suffix to the filename after modifying them. If given as an empty "
        "string the original files will be overwritten. Default 'mod'"
    )
    parser.add_argument("--suffix", default="mod", help=suffix_help)
    args = parser.parse_args()

    for f in glob.glob(args.file):
        if f.endswith("xml"):
            print(f"Updating {f}")
            tree = et.parse(f)  # nosec
            root = tree.getroot()
            root.set("xml:base", args.baseURI)

            out_file = f
            if args.suffix:
                out_file = out_file.split(".")[0] + f"{args.suffix}.xml"
            tree.write(out_file)


main()
