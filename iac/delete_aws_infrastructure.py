import boto3
import configparser

from redshift import delete_redshift_cluster
from iam import delete_iam_role

def main():
    """
    Main function for infrastructure as code deletion. It reads the config file, deletes the aws infrastructure (iam role, redshift db).
    """
    # Load DWH Params from a file
    config = configparser.ConfigParser()
    config.read_file(open('./dwh.cfg'))

    IDENTIFIER              = config.get("CLUSTER","IDENTIFIER")
    CLUSTER_IAM_ROLE_NAME   = config.get("CLUSTER", "CLUSTER_IAM_ROLE_NAME")
    
    # Load AWS credentials from a file
    config.read_file(open('./aws.cfg'))
    
    KEY                     = config.get('AWS','KEY')
    SECRET                  = config.get('AWS','SECRET')

    # Create clients for IAM and Redshift
    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    delete_iam_role(iam, CLUSTER_IAM_ROLE_NAME)
    delete_redshift_cluster(redshift, IDENTIFIER)

if __name__ == "__main__":
    main()
