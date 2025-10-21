from pyxnat import Interface
import os
from migration_report import get_experiment_file_stats
import time

def test_get_experiment_file_stats():
    """Test that get_experiment_file_stats returns a non-empty result."""
    
    # opens up a connection to xnat2.bu.edu. Assumes user has a proper configuration
    # file in their home directory.
    xnat_cfg = os.path.expanduser('~/xnat.cfg')
    xnat2_cfg = os.path.expanduser('~/xnat2.cfg')
    
    start_time = time.time()
    stats = get_experiment_file_stats(xnat_cfg, xnat2_cfg, 'CNC_Archive_E02462', 'BASS', 'CNC_Archive_S02250', '230814_BASSptp024')
    end_time = time.time()

    elapsed_time = end_time - start_time
    print(f"Function executed in {elapsed_time:.4f} seconds")
    print(stats)