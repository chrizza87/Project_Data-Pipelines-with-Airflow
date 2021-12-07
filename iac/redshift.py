def create_redshift_cluster(redshift, clusterType, nodeType, numNodes, dbName, clusterIdentifier, dbUser, dbPassword, iamRoleArn):
    """
    Creates an redshift cluster for given params

    Parameters
    ----------
    redshift: Boto3 redshift ressource object
    clusterType: Type of the cluster
    nodeType: Type of the nodes (single-node, multi-node)
    numNodes: Number of the nodes (used just for multi-node)
    dbName: Name of the database
    clusterIdentifier: Identifier of the cluster
    dbUser: User for the db
    dbPassword: Password for the user
    iamRoleArn: Iam role arn (needed for rights)
    
    Returns the endpoint address of the created redshift cluster
    """
    
    try:
        hw_params = {}
        hw_params['ClusterType'] = clusterType
        hw_params['NodeType'] = nodeType
        
        if (clusterType == 'multi-node'):
            hw_params['NumberOfNodes'] = int(numNodes)
                    
        response = redshift.create_cluster(        
            #HW
            **hw_params,

            #Identifiers & Credentials
            DBName=dbName,
            ClusterIdentifier=clusterIdentifier,
            MasterUsername=dbUser,
            MasterUserPassword=dbPassword,
            
            #Roles (for s3 access)
            IamRoles=[iamRoleArn]  
        )
        
        waiter = redshift.get_waiter('cluster_available')
        waiter.wait(ClusterIdentifier=clusterIdentifier)
        
        print('Redshift cluster created')
        
        cluster = response['Cluster']
        return cluster['Endpoint']['Address']
    except Exception as e:
        print(e)
        return get_redshift_cluster(redshift, clusterIdentifier)

def delete_redshift_cluster(redshift, clusterIdentifier):
    """
    Deletes an redshift cluster for given clusterIdentifier

    Parameters
    ----------
    redshift: Boto3 redshift ressource object
    clusterIdentifier: Identifier of the cluster
    """
    
    try:
        redshift.delete_cluster( ClusterIdentifier=clusterIdentifier,  SkipFinalClusterSnapshot=True)
        waiter = redshift.get_waiter('cluster_deleted')
        waiter.wait(ClusterIdentifier=clusterIdentifier)
        
        print('Redshift cluster deleted')
    except Exception as e:
        print(e)
        
def get_redshift_cluster(redshift, clusterIdentifier):
    """
    Gets the endpoint address of an redshift cluster for given clusterIdentifier

    Parameters
    ----------
    redshift: Boto3 redshift ressource object
    clusterIdentifier: Identifier of the cluster
    
    Returns the endpoint address of the redshift cluster
    """
    try:
        response = redshift.describe_clusters(ClusterIdentifier=clusterIdentifier)
        cluster = response['Clusters'][0]
        return cluster['Endpoint']['Address']
    except Exception as e:
        print(e)
