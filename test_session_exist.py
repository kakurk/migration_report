from pyxnat import Interface
import os
from migration_report import session_exists

def test_session_exists():
    # opens up a connection to xnat2.bu.edu. Assumes user has a proper configuration
    # file in their home directory.
    xnat = Interface(config=os.path.expanduser('~/xnat2.cfg'))

    # This session and project should exist in the xnat2 instance.
    session_label = '251007_QA'
    project = 'qa'    
    assert session_exists(xnat, session_label, project) == True
    xnat.disconnect()