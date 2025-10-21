import os
from pyxnat import Interface
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import time
import pandas as pd

# ----------- CONFIGURATION -----------
# This script uses pyxnat. pyxnat uses configuration files
# to authenticate with an xnat instance. These files have the
# following format:
# {"server": "https://xnat.bu.edu", "user": "USERNAME", "password": "PASSWORD"}
# See pyxnat documentation for more information.
XNAT1_CFG = os.path.expanduser('~/xnat.cfg')
XNAT2_CFG = os.path.expanduser('~/xnat2.cfg')
MAX_WORKERS=16
# -------------------------------------

# --- Worker Function to get file stats for a single experiment ---
def get_experiment_file_stats(xnat_interface_config_path, xnat2_config, experiment_id, project_id, subject_id, subject_label):
    """
    Connects to XNAT, fetches file statistics (count and total size) for a single experiment
    by iterating through its scans and their resources.
    Each worker thread will create its own pyxnat.Interface for thread safety.
    """
    thread_xnat_interface = None
    try:
        thread_xnat_interface = Interface(config=xnat_interface_config_path)
        
        total_experiment_files = 0
        total_experiment_size_bytes = 0

        # Step 1: Get all scans for this experiment
        # API call: /data/experiments/{experiment_id}/scans
        response = thread_xnat_interface.get(f'/data/experiments/{experiment_id}/scans')
        assert response.ok, f"Failed to get scans for experiment {experiment_id}"
        scans_list = response.json()['ResultSet']['Result']

        # Step 2: For each scan, get its resources and aggregate file stats
        for scan in scans_list:

            # API call: /data/experiments/{experiment_id}/scans/{scan_id}/resources
            # scan['URI'] provides the URI for the scan: /data/experiments/{experiment_id}/scans/{scan_id}
            URI = scan['URI']
            resources_resp = thread_xnat_interface.get(f'{URI}/resources')
            assert resources_resp.ok, f"Failed to get resources for scan {scan['ID']} in experiment {experiment_id}"    
            resources_list = resources_resp.json()['ResultSet']['Result']

            # Sum 'file_size' and 'file_count' from the resources of this scan
            # Handle potential NaN values if some resources don't have these fields
            total_experiment_files += sum((int(res.get('file_count', 0)) for res in resources_list))
            total_experiment_size_bytes += sum((int(res.get('file_size', 0)) for res in resources_list))

        # step 3: create a mapping of experiment IDs --> experiment Labels
        resp = thread_xnat_interface.get('/data/experiments?columns=ID,label')
        list_of_dicts = resp.json()['ResultSet']['Result']
        id_to_label_map = {d["ID"]: d["label"] for d in list_of_dicts}
        experiment_label = id_to_label_map.get(experiment_id, 'Label Not Found')

        # step 4: Does this experiment exist in XNAT2?
        xnat2_interface = Interface(config=xnat2_config)
        xnat2_exists = xnat2_interface.select.experiment(experiment_label).exists()

        return {
            'project_id': project_id,
            'subject_id': subject_id,
            'subject_label': subject_label,
            'experiment_id': experiment_id,
            'experiment_label': experiment_label,
            'xnat2_exists': xnat2_exists,
            'num_files': int(total_experiment_files), # Ensure integer
            'total_size_bytes': int(total_experiment_size_bytes) # Ensure integer
        }
    except Exception as e:
        print(f"Error processing experiment {experiment_id}: {e}", flush=True)
        return {
            'project_id': project_id,
            'subject_id': subject_id,
            'suject_label': subject_label,
            'experiment_id': experiment_id,
            'experiment_label': experiment_label,
            'num_files': None,
            'total_size_bytes': None,
            'error': str(e)
        }
    finally:
        if thread_xnat_interface:
            try:
                thread_xnat_interface.disconnect()
            except Exception as e:
                print(f"Error disconnecting XNAT1 interface for experiment {experiment_id}: {e}", flush=True)
        if xnat2_interface:
            try:
                xnat2_interface.disconnect()
            except Exception as e:
                print(f"Error disconnecting XNAT2 interface for experiment {experiment_id}: {e}", flush=True)

def get_session_info(exp):
    """Returns session info: [(session_label, project, investigator)]"""
    project_label = exp.parent().parent().label()
    subject_label = exp.parent().label()
    pi_lastname = exp.parent().parent().attrs.get('pi_lastname')
    pi_firstname = exp.parent().parent().attrs.get('pi_firstname')
    experiment_label = exp.label()
    resources = exp.scans().resources()
    file_size = 0
    file_count = 0
    for res in resources:
        res_attributes = res.attributes()
        file_size += int(res_attributes['file_size'])
        file_count += int(res_attributes['file_count'])
    return project_label, pi_firstname, pi_lastname, subject_label, experiment_label, file_count, file_size

def session_exists(xnat_interface, session_label):
    return xnat_interface.select.experiment(session_label).exists()

def main():

    start_time = time.time()

    xnat1 = Interface(config=XNAT1_CFG)

    # select all of the MRI scan sessions (in xnat terminology, the "experiments") currently on xnat1
    search_object = xnat1.select('xnat:mrSessionData', ['xnat:mrSessionData/PROJECT','xnat:mrSessionData/SUBJECT_ID', 'xnat:mrSessionData/SESSION_ID', 'xnat:mrSessionData/SUBJECT_LABEL'])
    experiments_table = search_object.all()

    xnat1.disconnect()

    experiment_tasks = []
    for row in experiments_table:
        experiment_id = row['session_id']
        project_id = row['project']
        subject_id = row['subject_id']
        subject_label = row['subject_label']
        experiment_tasks.append((experiment_id, project_id, subject_id, subject_label))

    print(f"Found {len(experiment_tasks)} experiments to process for file statistics.")
    print(f"Processing experiment file stats using {MAX_WORKERS} worker threads concurrently...")

    results = []
    processed_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit tasks and map experiment_tasks to get_experiment_file_stats
        futures = {executor.submit(get_experiment_file_stats, XNAT1_CFG, XNAT2_CFG, experiment_id, project_id, subject_id, subject_label): (experiment_id, project_id, subject_id, subject_label)
                   for experiment_id, project_id, subject_id, subject_label in experiment_tasks}
        
        for future in as_completed(futures):
            processed_count += 1
            exp_id_current, _, _, _ = futures[future] # Get experiment ID for logging progress
            try:
                result = future.result()
                results.append(result)
                if processed_count % 10 == 0 or processed_count == len(experiment_tasks): # Print every 5 tasks, and always print the final one
                    print(f"Processed {processed_count}/{len(experiment_tasks)} experiments. Current: {exp_id_current}", flush=True)
            except Exception as e:
                print(f"A task for experiment {exp_id_current} failed to complete: {e}", flush=True)
                results.append(None) # Append None or an error dict for failed tasks

    end_time = time.time()
    print(f"\nFinished processing {len(experiment_tasks)} experiments in {end_time - start_time:.2f} seconds.")

    successful_results = [r for r in results if r is not None and 'error' not in r]

    if successful_results:
        df_results = pd.DataFrame(successful_results)
        print("\nSample Results:")
        print(df_results.head())
        
        total_estimated_files = df_results['num_files'].sum()
        total_estimated_size_gb = df_results['total_size_bytes'].sum() / (1024**3)
        
        print(f"\nTotal estimated files across all successful experiments: {total_estimated_files}")
        print(f"Total estimated size across all successful experiments: {total_estimated_size_gb:.2f} GB")
        
        output_filename = "xnat_experiment_file_stats_with_scans.csv"
        df_results.to_csv(output_filename, index=False)
        print(f"Detailed results saved to {output_filename}")
    else:
        print("No successful results to display or save.")

if __name__ == '__main__':
    main()