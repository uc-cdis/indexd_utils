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

    prefix = prefix or ""
    for fi in files:
        try:
            doc = indexclient.get(prefix + "/" + fi.get("id"))
            url = fi.get("url")
            if doc is not None:
                need_update = False
                if url not in doc.urls:
                    doc.urls.append(url)
                    need_update = True

                if fi.get("acl") in {"[u'open']", "['open']"}:
                    acl = ["*"]
                else:
                    acl = [element.strip() for element in fi.get("acl")[1:-1].split(",")]

                if doc.acl != acl:
                    doc.acl = acl
                    need_update = True

                if need_update:
                    doc.patch()
            else:
                doc = indexclient.create(
                        did=prefix + "/" + fi.get("id"),
                        hashes={"md5": fi.get("md5")},
                        size=fi.get("size", 0),
                        acl=acl,
                        urls=[url],
                    )

        except Exception as e:
            # Don't break for any reason
            logger.error("Can not update/create an indexd record with uuid {}. Detail {}".format(fi.get("id"))


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    indexing_cmd = subparsers.add_parser("indexing")
    indexing_cmd.add_argument("--prefix", required=True, help="indexd prefix")
    indexing_cmd.add_argument("--manifest", required=True, help="The manifest path")


if __name__ == '__main__':
    args = parse_arguments()

    if args.action == "indexing":
        submit_test_data(args.host, args.project, args.dir, args.access_token_file, int(args.chunk_size))
        return
