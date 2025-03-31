def retrieve(url, fname, path, force=False):
    import os

    import pooch

    # Create the full path to the file
    full_path = os.path.join(os.path.expanduser(path), fname)
    # Check if the file exists locally and delete if redownload is forced
    if os.path.isfile(full_path) and force:
        print(f"Removing existing file '{full_path}'")
        os.remove(full_path)

    filename = pooch.retrieve(
        url=url,
        fname=fname,
        known_hash=None,
        path=path,
    )
    return filename
