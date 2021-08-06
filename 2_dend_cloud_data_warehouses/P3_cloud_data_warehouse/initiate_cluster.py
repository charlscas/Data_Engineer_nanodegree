from os import wait
import pandas as pd
import boto3
import json
import time

import configparser
from botocore.exceptions import ClientError

PATH = './dwh.cfg'

def getEmptySections(config):
    empty_sections = []
    for section in config:
        for (each_key, each_val) in config.items(section):
            if not each_val and each_key != 'DWH_ENDPOINT':
                print("Warning: ", each_key, " is empty in [", section, "]", sep="")
                if section not in empty_sections:
                    empty_sections.append(section)
    
    return empty_sections

def getPrettyParameters(config):
    df = pd.DataFrame({"Param":
                  [
                      "DWH_CLUSTER_TYPE", 
                      "DWH_NUM_NODES", 
                      "DWH_NODE_TYPE", 
                      "DWH_CLUSTER_IDENTIFIER", 
                      "DWH_DB", "DWH_DB_USER", 
                      "DWH_DB_PASSWORD", 
                      "DWH_PORT", 
                      "DWH_IAM_ROLE_NAME"],
              "Value":
                  [
                    config['DWH']['DWH_CLUSTER_TYPE'], 
                    config['DWH']['DWH_NUM_NODES'], 
                    config['DWH']['DWH_NODE_TYPE'], 
                    config['DWH']['DWH_CLUSTER_IDENTIFIER'], 
                    config['DWH']['DWH_DB'], 
                    config['DWH']['DWH_DB_USER'], 
                    config['DWH']['DWH_DB_PASSWORD'], 
                    config['DWH']['DWH_PORT'], 
                    config['DWH']['DWH_IAM_ROLE_NAME']
                  ]
             })
    print('-------------------------------------', '1. DWH Parameters:', sep="\n")
    print(df)
    print('-------------------------------------')

def createClients(region_name, key, secret):
    try:
        print("2. Clients")
        ec2 = boto3.resource('ec2',
            region_name=region_name,
            aws_access_key_id=key,
            aws_secret_access_key=secret
        )
        print('- EC2 client created')

        s3 = boto3.resource('s3',
            region_name=region_name,
            aws_access_key_id=key,
            aws_secret_access_key=secret
        )
        print('- S3 client created')

        iam = boto3.client('iam',
            region_name=region_name,
            aws_access_key_id=key,
            aws_secret_access_key=secret
        )
        print('- IAM client created')

        redshift = boto3.client('redshift',
            region_name=region_name,
            aws_access_key_id=key,
            aws_secret_access_key=secret
        )
        print('- Redshift client created')

        print('-------------------------------------')

        return ec2, s3, iam, redshift
    except Exception as e:
        print(e)

def createRole(iam,role_name):
    try:
        print("3. Role")
        print("3.1 Creating a new IAM Role:", role_name) 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
    except ClientError as e:
        if "EntityAlreadyExists" in e.response['Error']['Code']:
            print("- Role already exists")
        else:
            print(e)       
    except Exception as e:
        print(e)

def deleteRole(iam, role_name):
    try:
        print("Deleting role")
        iam.delete_role(RoleName=role_name)
    except ClientError as e:
        if "NoSuchEntity" in e.response['Error']['Code']:
            print("- Role is already deleted")
        else:
            print(e)
    except Exception as e:
        print(e)

    
def attachPolicyToRole(iam, config):
    try:
        print("3.2 Attaching Policy")
        iam.attach_role_policy(RoleName=config['DWH']['DWH_IAM_ROLE_NAME'],
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']

        arn = iam.get_role(RoleName=config['DWH']['DWH_IAM_ROLE_NAME'])['Role']['Arn']
        print("3.3 Write the IAM role ARN:", arn)
        config['IAM_ROLE']['ARN'] = arn
        with open(PATH, 'w') as conf:
            config.write(conf)
        print('-------------------------------------')

    except Exception as e:
        print(e)

def detachPolicyToRole(iam, config):
    try:
        print("Detaching Policy")
        iam.detach_role_policy(RoleName=config['DWH']['DWH_IAM_ROLE_NAME'], 
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

        print("Deleting ARN from '", PATH[2:], "' file", sep='')
        config['IAM_ROLE']['ARN'] = ''
        with open(PATH, 'w') as conf:
            config.write(conf)
    except ClientError as e:
        if "NoSuchEntity" in e.response['Error']['Code']:
            print("- Role is already deleted")
        else:
            print(e)        
    except Exception as e:
        print(e)

def createRedshiftCluster(redshift, config):
    try:
        print('4. Redshift')
        print('- Creating Redshift cluster:')
        response = redshift.create_cluster(        
            ClusterType=    config['DWH']['DWH_CLUSTER_TYPE'],
            NodeType=       config['DWH']['DWH_NODE_TYPE'],
            NumberOfNodes=  int(config['DWH']['DWH_NUM_NODES']),

            DBName=config['DWH']['DWH_DB'],
            ClusterIdentifier=config['DWH']['DWH_CLUSTER_IDENTIFIER'],
            MasterUsername=config['DWH']['DWH_DB_USER'],
            MasterUserPassword=config['DWH']['DWH_DB_PASSWORD'],
            
            IamRoles=[config['IAM_ROLE']['ARN']]  
        )
    except ClientError as e:
        if "ClusterAlreadyExists" in e.response['Error']['Code']:
            print("Redshift cluster '",config['DWH']['DWH_CLUSTER_IDENTIFIER'],"' already exists", sep='')
        else:
            print(e)
    except Exception as e:
        print(e)

def deleteRedshiftCluster(redshift, config):
    try:
        print('-------------------------------------')
        print('Deleting Redshift cluster')
        redshift.delete_cluster( ClusterIdentifier=config['DWH']['DWH_CLUSTER_IDENTIFIER'],  SkipFinalClusterSnapshot=True)
        config['DWH']['DWH_ENDPOINT'] = ""
        print('Deleting endpoint from', PATH[2:], 'file')
        with open(PATH, 'w') as conf:
            config.write(conf)
    except ClientError as e:
        if "ClusterNotFound" in e.response['Error']['Code']:
            print("- Redshift cluster is already deleted")
        else:
            print(e)        
        print('Deleting endpoint from', PATH[2:], 'file')
        config['DWH']['DWH_ENDPOINT'] = ""
        with open(PATH, 'w') as conf:
            config.write(conf)
    except Exception as e:
        print(e)

def describeRedshiftCluster(redshift, config):
    try:
        pd.set_option('display.max_colwidth', None)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", 
                "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=config['DWH']['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
        x = [(k, v) for k,v in myClusterProps.items() if k in keysToShow]
        print(pd.DataFrame(data=x, columns=["Key", "Value"]))
        print('-------------------------------------')
    except Exception as e:
        print(e)

def getRedshiftClusterProperties(redshift, config):
    return redshift.describe_clusters(ClusterIdentifier=config['DWH']['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]

def getStatusOfRedshiftCluster(redshift, config):
    return getRedshiftClusterProperties(redshift, config)['ClusterStatus']

def isRedshiftClusterAvailable(redshift, config):
    return (getStatusOfRedshiftCluster(redshift, config) == 'available')

def isRedshiftClusterDeleted(redshift, config):
    try:
        time_passed = 1
        while getStatusOfRedshiftCluster(redshift, config) != 'available':
            print("Redshift Cluster Status:", getStatusOfRedshiftCluster(redshift, config), "-", time_passed, "s")
            time.sleep(10)
            time_passed += 10
    except ClientError as e:
        if "ClusterNotFound" in e.response['Error']['Code']:
            print("- Redshift cluster is now deleted")
        else:
            print("Here", e)
    except Exception as e:
        print(e)
    
def waitUntilRedshiftClusterIsAvailable(redshift,config):
    time_passed = 0
    print("5. Creating cluster:")
    while not(isRedshiftClusterAvailable(redshift,config)):
        time.sleep(2)
        time_passed += 2
        if time_passed % 30 == 0:
            print("* ClusterStatus at ", time_passed, "s: ", 
                getStatusOfRedshiftCluster(redshift,config), sep="")
    print("- ClusterStatus at ", time_passed, "s: ", 
                getStatusOfRedshiftCluster(redshift,config), sep="")
    print('-------------------------------------')
    describeRedshiftCluster(redshift,config)
    print("6. Saving hostname in", PATH[2:], "file")
    config['DWH']['DWH_ENDPOINT'] = redshift.describe_clusters(ClusterIdentifier=config['DWH']['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]['Endpoint']['Address']
    with open(PATH, 'w') as conf:
        config.write(conf)
    print('-------------------------------------')

def openIncomingTCPPort(ec2,redshift,config):
    try:
        print("7. VPC - Security group:")
        vpc = ec2.Vpc(id=getRedshiftClusterProperties(redshift,config)['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print("Security Group:", defaultSg.group_name," - ", defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config['DWH']['DWH_PORT']),
            ToPort=int(config['DWH']['DWH_PORT'])
        )
        print('-------------------------------------')
    except ClientError as e:
        if "InvalidPermission.Duplicate" in e.response['Error']['Code']:
            print("- The specified rule 'peer: 0.0.0.0/0, TCP, from port: 5439, to port: 5439, ALLOW' already exists")
        else:
            print("Here", e)
    except Exception as e:
        print(e)    

def main():
    """
    First, we read the parameters from the dwh.cfg file.
    Second, we create the clients for EC2, S2, IAM and Redshift.
    Third, we create the role and the policy for the DWH.
    Fouth, we create the cluster in Redshift and wait until it is available.
    Then, we open the incoming TCP port.
    """
    config = configparser.ConfigParser()
    config.optionxform=str
    config.read(PATH)
    empty_sections = getEmptySections(config)
    getPrettyParameters(config)

    if 'AWS' in empty_sections:
        print('WARNING: There are empty values in the [AWS] section')
    ec2, s3, iam, redshift = createClients(
        config['AWS']['REGION_NAME'], 
        config['AWS']['KEY'], 
        config['AWS']['SECRET']
    )

    if 'DWH' in empty_sections:
        print('WARNING: There are empty values in the [DWH] section')
    createRole(iam, config['DWH']['DWH_IAM_ROLE_NAME'])
    time.sleep(1)
    attachPolicyToRole(iam, config)

    createRedshiftCluster(redshift, config)
    time.sleep(5)
    describeRedshiftCluster(redshift,config)
    waitUntilRedshiftClusterIsAvailable(redshift,config)

    openIncomingTCPPort(ec2,redshift,config)

    # deleteRedshiftCluster(redshift,config)
    # time.sleep(2)
    # detachPolicyToRole(iam, config)
    # deleteRole(iam, config['DWH']['DWH_IAM_ROLE_NAME'])
    # isRedshiftClusterDeleted(redshift,config)


if __name__ == "__main__":
    main()
