import pandas as pd
import boto3
from botocore.exceptions import ClientError
import create_redshift as cr

def prettyRedshiftProps(props):
    '''
        Helper function to retrieve cluster properties

        Parameters:
            props: a list of properties to retrieve from the cluster

        Returns:
            a pandas dataframe showing the cluster key and value for each requested property
    '''
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


# run these lines to delete the redshift cluster and 
# clean up the iam roles
def redshift_cleanup():
    '''
        Convenience function to easily delete the redshift cluster after
        user is finished testing
    '''
    cr.redshift.delete_cluster( ClusterIdentifier=cr.DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

    myClusterProps = cr.redshift.describe_clusters(ClusterIdentifier=cr.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)

    cr.iam.detach_role_policy(RoleName=cr.DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    cr.iam.delete_role(RoleName=cr.DWH_IAM_ROLE_NAME)


redshift_cleanup()