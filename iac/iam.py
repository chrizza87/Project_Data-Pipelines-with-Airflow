import json

def create_iam_role(iam, iamRoleName):
    """
    Creates an iam role for given iamRoleName

    Parameters
    ----------
    iam: Boto3 iam ressource object
    iamRoleName: Name of the iam role
    
    Returns the created iam role arn
    """
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=iamRoleName,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
    iam.attach_role_policy(RoleName=iamRoleName,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']
    
    print('IAM role created')

    return iam.get_role(RoleName=iamRoleName)['Role']['Arn']

def delete_iam_role(iam, iamRoleName):
    """
    Deletes an iam role for given iamRoleName

    Parameters
    ----------
    iam: Boto3 iam ressource object
    iamRoleName: Name of the iam role
    """
    try:
        iam.detach_role_policy(RoleName=iamRoleName, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=iamRoleName)
        print('IAM role deleted')

    except Exception as e:
        print(e)
        
def get_iam_role_arn(iam, iamRoleName):
    """
    Gets the iam role arn for given iamRoleName

    Parameters
    ----------
    iam: Boto3 iam ressource object
    iamRoleName: Name of the iam role
    
    Returns the iam role arn
    """
    return iam.get_role(RoleName=iamRoleName)['Role']['Arn']
    