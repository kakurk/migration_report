import os
from pyxnat import Interface
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import time
import pdb

# ----------- CONFIGURATION -----------
# This script uses pyxnat. pyxnat uses configuration files
# to authenticate with an xnat instance. These files have the
# following format:
# {"server": "https://xnat.bu.edu", "user": "USERNAME", "password": "PASSWORD"}
# See pyxnat documentation for more information.
XNAT1_CFG = os.path.expanduser('~/xnat.cfg')
XNAT2_CFG = os.path.expanduser('~/xnat2.cfg')
# -------------------------------------

# --- Worker Function to get file stats for a single experiment ---
def get_experiment_file_stats(xnat_interface_config_path, experiment_id, project_id, subject_id, subject_label):
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
        pdb.set_trace()
        scans_list = response.json()['ResultsSet']['Result']

        # Step 2: For each scan, get its resources and aggregate file stats
        for scan in scans_list:

            # API call: /data/experiments/{experiment_id}/scans/{scan_id}/resources
            # scan['URI'] provides the URI for the scan: /data/experiments/{experiment_id}/scans/{scan_id}
            URI = scan['URI']
            resources_resp = thread_xnat_interface.get(f'{URI}/resources')
            assert resources_resp.ok, f"Failed to get resources for scan {scan['ID']} in experiment {experiment_id}"    
            resources_list = resources_resp.json()['ResultsSet']['Result']

            # Sum 'file_size' and 'file_count' from the resources of this scan
            # Handle potential NaN values if some resources don't have these fields
            total_experiment_files += sum(res.get('file_count', 0) for res in resources_list)
            total_experiment_size_bytes += sum(res.get('file_size', 0) for res in resources_list)

        return {
            'project_id': project_id,
            'subject_id': subject_id,
            'experiment_id': experiment_id,
            'num_files': int(total_experiment_files), # Ensure integer
            'total_size_bytes': int(total_experiment_size_bytes) # Ensure integer
        }
    except Exception as e:
        print(f"Error processing experiment {experiment_id}: {e}", flush=True)
        return {
            'project_id': project_id,
            'subject_id': subject_id,
            'experiment_id': experiment_id,
            'num_files': None,
            'total_size_bytes': None,
            'error': str(e)
        }
    finally:
        if thread_xnat_interface:
            try:
                thread_xnat_interface.disconnect()
            except Exception as e:
                print(f"Error disconnecting XNAT interface for experiment {experiment_id}: {e}", flush=True)

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

    # generate the migration report

    xnat1 = Interface(config=XNAT1_CFG)
    xnat2 = Interface(config=XNAT2_CFG)

    # select all of the MRI scan sessions (in xnat terminology, the "experiments") currently on xnat1
    search_object = xnat1.select('xnat:mrSessionData', ['xnat:mrSessionData/PROJECT','xnat:mrSessionData/SUBJECT_ID', 'xnat:mrSessionData/SESSION_ID'])
    experiments_table = search_object.all()

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
        futures = {executor.submit(get_experiment_file_stats, XNAT2_CFG, experiment_id, project_id, subject_id, subject_label): (experiment_id, project_id, subject_id, subject_label)
                   for experiment_id, project_id, subject_id, subject_label in experiment_tasks}
        
        for future in as_completed(futures):
            processed_count += 1
            exp_id_current, _, _ = futures[future] # Get experiment ID for logging progress
            try:
                result = future.result()
                results.append(result)
                if processed_count % 10 == 0: # Print progress more frequently
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







    experiments = xnat1.select.projects().subjects().experiments()
    
    output_csv_filename = os.path.expanduser('~/xnat_migration_report.csv')
    fieldnames = ['project_label', 'pi_firstname', 'pi_lastname', 'subject_label', 'experiment_label', 'file_count', 'file_size', 'exists_on_xnat2']

    # for each experiment found...
    c = 0
    for exp in experiments:

        # check if this session is already in the report (skip if so)

        # extract key information about that session
        project_label, pi_firstname, pi_lastname, subject_label, experiment_label, file_count, file_size = get_session_info(exp)

        c += 1
        print(f"Processing session {c}/4030: {experiment_label}...")

        # does this session exist on xnat2?
        exists_on_xnat2 = session_exists(xnat2, experiment_label)
    
        # append to the report
        report_data.append({
            'project_label': project_label,
            'pi_firstname': pi_firstname,
            'pi_lastname': pi_lastname,
            'subject_label': subject_label,
            'experiment_label': experiment_label,
            'file_count': file_count,
            'file_size': file_size,
            'exists_on_xnat2': exists_on_xnat2
        })

    # write the report to a CSV file
    df_report = pd.DataFrame(report_data)

    # Define the output CSV file name
    output_csv_filename = os.path.expanduser('~/xnat_migration_report.csv')

    # Write the DataFrame to a CSV file
    # index=False prevents pandas from writing the DataFrame index as a column in the CSV
    df_report.to_csv(output_csv_filename, index=False)

if __name__ == '__main__':
    main()