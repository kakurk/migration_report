from pyxnat import Interface
import os
from migration_report import get_experiment_file_stats
import time

def test_get_experiment_file_stats():
    """Test that get_experiment_file_stats returns a non-empty result."""
    
    # opens up a connection to xnat2.bu.edu. Assumes user has a proper configuration
    # file in their home directory.
    xnat2_cfg = os.path.expanduser('~/xnat2.cfg')
    
    start_time = time.time()
    stats = get_experiment_file_stats(xnat2_cfg, 'BU_CNC_E00010', 'qa', 'BU_CNC_S00008', '240715_QA')
    end_time = time.time()

    elapsed_time = end_time - start_time
    print(f"Function executed in {elapsed_time:.4f} seconds")
    print(stats)