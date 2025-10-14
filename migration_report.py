import os
from pyxnat import Interface
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

# ----------- CONFIGURATION -----------
# This script uses pyxnat. pyxnat uses configuration files
# to authenticate with an xnat instance. These files have the
# following format:
# {"server": "https://xnat.bu.edu", "user": "USERNAME", "password": "PASSWORD"}
# See pyxnat documentation for more information.
XNAT1_CFG = os.path.expanduser('~/xnat.cfg')
XNAT2_CFG = os.path.expanduser('~/xnat2.cfg')
# The Cognitive Neuroimaging Center has an independent workflow for archiving data
# that gets collected the the center. This workflow involves automatically pushing
# to a network file storage location that is mounted to the directory /cnc on the
# xnat2.bu.edu VM. In a succinct summary, the workflow is as follows:
# 1) Data automatically pushed from the computer at the scanner -- a computer that
#    has limited storage -- to a network file storage location nas-ru1.bu.edu.
# 2) Data is copied to the directory nas-ru1.bu.edu:/DATA/tmp
# 3) A daily cron job on XNAT2 reads the header information from these raw DICOM files
#    and moves that data to the archive located at: 
#    /DATA/INVESTIGATORS/INVESTIGATOR_{INVESTIGATOR_NAME}/{SESSION_LABEL}.tar.gz
# 4) A daily cron job on XNAT identifies directories within the archive that are older
#    than 2 weeks old and attempts to create a tar ball and compress that directory.
CNC_ARCHIVE_DIR = '/cnc/DATA/INVESTIGATORS'
# -------------------------------------

def get_sessions(xnat_interface):
    """Returns session info: [(session_label, project, investigator)]"""
    sessions = []
    projects = xnat_interface.select.projects()
    for project in projects:
        project_label = project.label()
        # Get owner/investigator from project meta (customize if needed)
        try:
            invest_name = project.attrs.get('pi_lastname')
        except Exception:
            invest_name = 'UNKNOWN'
        for subject in project.subjects():
            subject_label = subject.label()
            for experiment in subject.experiments():
                session_label = experiment.label()
                session_size = estimate_session_size(experiment)
                sessions.append((project_label, invest_name, subject_label, session_label, session_size))
    return sessions

def estimate_session_size_orig(experiment):
    total_size = 0
    for scan in experiment.scans():
        for resource in scan.resources():
            for file in resource.files():
                file_info = file.attributes()
                total_size += int(file_info['Size'])

def estimate_session_size_alt(experiment):
    total_size = 0
    all_files = experiment.scans().resources().files()
    for f in all_files:
        total_size += int(f.attributes()['Size'])

def estimate_session_size(experiment, use_threads: bool = False, max_workers: Optional[int] = 8) -> int:
    """
    Estimate total size (in bytes) of all files in an experiment.

    Behavior:
    - Traverses the experiment once to collect file objects.
    - Reads each file's attributes to find a size field (tries several common keys).
    - Returns total bytes as an int. On errors for individual files, treats that file as size 0.
    - If use_threads=True, will fetch file attributes in parallel using ThreadPoolExecutor
      (useful when the XNAT server latency dominates; keep default False to preserve
      sequential behavior and reduce load on the server).

    Args:
      experiment: pyxnat experiment selector object
      use_threads: whether to fetch file.attributes() calls concurrently
      max_workers: number of threads to use when use_threads=True

    Returns:
      total size in bytes (int)
    """
    # First, collect all file objects in one traversal to avoid repeated selector
    # state changes and make parallel attribute fetching easier.
    files = []
    try:
        for scan in experiment.scans():
            for resource in scan.resources():
                for f in resource.files():
                    files.append(f)
    except Exception:
        # If traversal fails (e.g. permission/selector errors), return 0 as unknown/empty.
        return 0

    def _file_size(f):
        """Return size for a single file object or 0 on any error."""
        try:
            info = f.attributes()
            # Try several common attribute names (different XNAT installs use different keys)
            size_val = (
                info.get('Size')
                or info.get('size')
                or info.get('FileSize')
                or info.get('length')
                or info.get('bytes')
            )
            if size_val is None:
                return 0
            return int(size_val)
        except Exception:
            # Swallow per-file errors to keep the whole calculation resilient.
            return 0

    total = 0
    if use_threads and files:
        # Parallelize attribute fetching to hide network latency. Use a modest
        # default for max_workers so we don't overload the server.
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_file_size, f) for f in files]
            for fut in as_completed(futures):
                try:
                    total += fut.result()
                except Exception:
                    # Shouldn't happen because _file_size swallows errors, but be safe.
                    continue
    else:
        # Sequential fallback (default) — predictable, less load on server.
        for f in files:
            total += _file_size(f)

    return total

def session_exists(xnat_interface, session_label, project):
    project = xnat_interface.select.project(project)
    for subject in project.subjects():
        try:
            for exp in subject.experiments():
                exp_label = exp.label()
                if exp_label == session_label:
                    return True
        except Exception as e:
            print(f"Warning: skipping subject {subject.id()} due to error: {e}")
    return False

def archive_exists(data_dir, invest_name, session_label):
    """Checks if session archive file exists locally."""
    pattern = os.path.join(
        data_dir,
        f'INVESTIGATOR_{invest_name}',
        f'{session_label}.tar.gz'
    )
    return os.path.isfile(pattern), pattern

# how much disk space does this scan session take up?


def main():

    xnat1 = Interface(config=XNAT1_CFG)
    xnat2 = Interface(config=XNAT2_CFG)

    sessions = get_sessions(xnat1)
    report = []

    for session_label, project, invest_name in sessions:
        exists_on_xnat2 = session_exists(xnat2, session_label, project)
        archive_exists_flag, archive_path = archive_exists(CNC_ARCHIVE_DIR, invest_name, session_label)
        report.append({
            'session_label': session_label,
            'project': project,
            'investigator': invest_name,
            'exists_on_xnat2': exists_on_xnat2,
            'archive_exists': archive_exists_flag,
            'archive_path': archive_path if archive_exists_flag else None
        })

    # Print results
    for entry in report:
        print(entry)

if __name__ == '__main__':
    main()