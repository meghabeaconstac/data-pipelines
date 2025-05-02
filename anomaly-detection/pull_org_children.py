import os
import csv
import boto3
import pyminizip
import shutil
from django.conf import settings
from app.models import Organization  # Update to your actual app name


print("Pulls child orgs for batches and uploads to S3")
BASE_FOLDER = '/tmp/custom_org_pull'
os.makedirs(BASE_FOLDER, exist_ok=True)
def compress_and_upload_to_s3():
    result_paths = [os.path.join(BASE_FOLDER, file) for file in os.listdir(BASE_FOLDER)]
    zip_path = os.path.join(BASE_FOLDER, 'custom_pull.zip')

    pyminizip.compress_multiple(result_paths, ['' for _ in result_paths], zip_path, None, 9)

    s3 = boto3.client('s3')
    #change the key and bucketname
    upload_key = f'your_folder/file_name.zip'
    bucket_name='yourbucketname'
    with open(zip_path, 'rb') as result_file:
        s3.upload_fileobj(result_file, bucket_name,upload_key)

    presigned_url = s3.generate_presigned_url('get_object',
        Params={
            "Bucket": bucket_name,
            "Key": upload_key
        }, ExpiresIn=900)

    print(f"Presigned URL: {presigned_url}")

    # Cleanup
    shutil.rmtree(BASE_FOLDER)

batches = {
    "batch1": ['123','345'],
    "batch2":['567','789']
}

for name, org_ids in batches.items():
    output_file = os.path.join(BASE_FOLDER, f"{name}.csv")
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Main Org ID', 'Child Org ID'])
        for org_id in org_ids:
            if not org_id:
                continue
            try:
                org = Organization.objects.get(id=org_id)
                main_org = org.main_organization
                child_ids = list(
                    Organization.objects.filter(parent=main_org).values_list('id', flat=True)
                )
                writer.writerow([main_org.id, main_org.id])  # Include itself
                for cid in child_ids:
                    writer.writerow([main_org.id, cid])
            except Organization.DoesNotExist:
                print(f"Organization {org_id} not found.")

# Compress and upload
compress_and_upload_to_s3()
print("Child organizations extracted and uploaded successfully.")


