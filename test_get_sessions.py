from pyxnat import Interface
import os
from migration_report import get_sessions

def test_get_sessions():
    """Test that get_sessions returns a non-empty list of sessions."""
    
    # opens up a connection to xnat2.bu.edu. Assumes user has a proper configuration
    # file in their home directory.
    xnat = Interface(config=os.path.expanduser('~/xnat2.cfg'))
    
    sessions = get_sessions(xnat)
    
    xnat.disconnect()

    assert len(sessions) > 0