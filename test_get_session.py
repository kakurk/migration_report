from pyxnat import Interface
import os
from migration_report import get_session
import time

def test_get_session():
    """Test that get_sessions returns a non-empty list of sessions."""
    
    # opens up a connection to xnat2.bu.edu. Assumes user has a proper configuration
    # file in their home directory.
    xnat = Interface(config=os.path.expanduser('~/xnat2.cfg'))
    
    experiment = xnat.select('/projects/qa/subjects/240715_QA/experiments/240715_QA')
    
    start_time = time.time()
    sessions = get_session(experiment)
    end_time = time.time()

    elapsed_time = end_time - start_time
    print(f"Function executed in {elapsed_time:.4f} seconds")
    print(f"Number of sessions retrieved: {len(sessions)}")

    xnat.disconnect()