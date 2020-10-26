import csv


def get_fileinfos_from_tsv_manifest(manifest_file, dem="\t"):
    """
    get file info from tsv manifest
    """
    files = []
    with open(manifest_file, "rt") as csvfile:
        csvReader = csv.DictReader(csvfile, delimiter=dem)
        for row in csvReader:
            row["size"] = int(row["size"])
            files.append(row)

    return files
