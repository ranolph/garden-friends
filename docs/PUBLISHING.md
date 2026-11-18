# Publishing Garden Friends to GitHub

The builder produces a new folder containing:

```text
garden-friends/             Repository contents, including artwork
garden-friends.zip          Complete distribution archive
garden-friends.zip.sha256   Archive checksum
```

Use the **contents of `garden-friends/`** as the repository. Keep the ZIP beside it for distribution as a release asset rather than committing the ZIP into the repository.

## Before the first upload

1. Run `python tools/verify_release.py` from inside `garden-friends/`.
2. Open `BUILD_REPORT.json`. It contains the number of images and files included, and the list of any files requiring Git LFS.
3. Choose the repository visibility and the intended code/artwork licences. The supplied `NOTICE.md` retains copyright; it does not grant an open-source licence automatically.
4. Keep the generated `.gitignore` and `.gitattributes` files. The runtime `data/` folder and real `.env` file must not be committed.

## GitHub Desktop

Create a local repository from the exported `garden-friends/` directory. Review its changes, make the initial commit and use **Publish repository**. Choose the intended visibility before publishing.

If `git_lfs_required` is `true`, install [Git LFS](https://git-lfs.com/) and initialise it in the repository before committing the files. The generated `.gitattributes` already identifies the oversized artwork paths:

```bash
git lfs install
```

## Git from a terminal

Create an empty GitHub repository named `garden-friends`, then use its actual repository URL below:

```bash
cd garden-friends
git init -b main
# If BUILD_REPORT.json says git_lfs_required is true:
# git lfs install
git add .
git commit -m "Introduce Garden Friends"
git remote add origin YOUR_REPOSITORY_URL
git push -u origin main
```

The URL is intentionally left for your chosen GitHub repository. The builder does not create repositories, change repository visibility or publish files on your behalf.

## File sizes and complete artwork

GitHub's browser uploader accepts individual files up to 25 MiB. Regular Git warns above 50 MiB and blocks files larger than 100 MiB. Git LFS handles files beyond the regular-Git limit; the export checks every file and writes exact-path LFS rules when required.

Use GitHub Desktop or Git for this artwork-heavy repository, even when all images fit the individual file limit. If LFS is required, do not remove the generated `.gitattributes` rules or stage the oversized files before installing LFS.

The ZIP contains the real artwork bytes. After publishing, you can attach `garden-friends.zip` and its `.sha256` file to a GitHub release. GitHub's release-asset limits and LFS quotas depend on the current account/plan; check the linked documentation for your repository.

Official references:

- [About large files on GitHub](https://docs.github.com/en/repositories/working-with-files/managing-large-files/about-large-files-on-github)
- [Configuring Git Large File Storage](https://docs.github.com/en/repositories/working-with-files/managing-large-files/configuring-git-large-file-storage)

## Rebuilding an export

From a Garden Friends source checkout, point the exporter at the complete static artwork directory and a new output path:

```bash
python tools/export_release.py \
  --artwork /path/to/species_art \
  --output /path/to/new-garden-friends-release
```

The exporter reads its inputs and writes only the new destination. Existing output folders are rejected instead of overwritten. Files that change while being copied cause the build to stop; rerun after artwork processing has finished.

The artwork gallery in the README is generated from the included images. `ARTWORK_MANIFEST.json` records their exact bytes, and `RELEASE_MANIFEST.json` verifies the whole exported project. Runtime snapshots, credentials, database files and backup folders are excluded from publication.
