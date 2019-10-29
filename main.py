import argparse

from indexclient.client import IndexClient
from cdislogging import get_logger

from settings import INDEXD
from utils import get_fileinfos_from_tsv_manifest

logger = get_logger(__name__)


def manifest_indexing(manifest, prefix=None):
    """
    Loop through all the files in the manifest, update/create records in indexd
    update indexd if the url is not in the record url list or acl has changed

    """
    indexclient = IndexClient(
        INDEXD["host"],
        INDEXD["version"],
        (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
    )
    try:
        files = get_fileinfos_from_tsv_manifest(manifest)
    except Exception as e:
        logger.error("Can not read {}. Detail {}".format(manifest, e))
        return

    prefix = prefix + "/" if prefix else ""
    number_indexed_files = 0
    for fi in files:
        try:
            urls = fi.get("url").split(" ")

            if fi.get("acl").lower() in {"[u'open']", "['open']"}:
                acl = ["*"]
            else:
                acl = [
                    element.strip().replace("'", "")
                    for element in fi.get("acl")[1:-1].split(",")
                ]

            doc = indexclient.get(prefix + fi.get("GUID"))
            if doc is not None:
                need_update = False
                if set(urls) != set(doc.urls):
                    doc.urls = urls
                    need_update = True

                if set(doc.acl) != set(acl):
                    doc.acl = acl
                    need_update = True

                if need_update:
                    doc.patch()
            else:
                doc = indexclient.create(
                    did=prefix + fi.get("GUID"),
                    hashes={"md5": fi.get("md5")},
                    size=fi.get("size", 0),
                    acl=acl,
                    urls=urls,
                )
            number_indexed_files += 1
            if number_indexed_files % 10 == 0 or number_indexed_files == len(files):
                logger.info(
                    "Progress {}%".format(number_indexed_files * 100.0 / len(files))
                )

        except Exception as e:
            # Don't break for any reason
            logger.error(
                "Can not update/create an indexd record with uuid {}. Detail {}".format(
                    fi.get("GUID")
                )
            )


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    indexing_cmd = subparsers.add_parser("indexing")
    indexing_cmd.add_argument("--prefix", required=True, help="indexd prefix")
    indexing_cmd.add_argument("--manifest", required=True, help="The manifest path")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    if args.action == "indexing":
        manifest_indexing(args.manifest, prefix=args.prefix)
