from pyxnat import Interface
import os
from migration_report import estimate_session_size
import time

def test_estimate_session_size():
    """Test that get_sessions returns a non-empty list of sessions."""
    
    # opens up a connection to xnat2.bu.edu. Assumes user has a proper configuration
    # file in their home directory.
    xnat = Interface(config=os.path.expanduser('~/xnat2.cfg'))
    # see the XNAT API and pyxnat documentation for the class "selector"
    # this selector takes the pattern:
    # /projects/{project_label}/subjects/{subject_label}/experiments/{experiment_label}
    experiment = xnat.select('/projects/qa/subjects/240715_QA/experiments/240715_QA')

    start_time = time.perf_counter()
    session_size = estimate_session_size(experiment, use_threads=True, max_workers=4)
    end_time = time.perf_counter()

    print(f"Time taken: {end_time - start_time} seconds")
    print(session_size)

    xnat.disconnect()

    assert session_size > 0