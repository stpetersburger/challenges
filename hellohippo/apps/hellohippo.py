"""
Script to process hellohippo files as test assignment
"""

import sys, os, argparse
from utils.udfs import *
import json


def run(args):
    pth = args.path + '/entrypoint'
    pth_output = pth + '/' + 'output'

    claims = pd.DataFrame()
    pharmacies = pd.DataFrame()
    reverts = pd.DataFrame

    for fld in os.listdir(pth):
        if fld in ['claims','pharmacies','reverts']:
            for fl in os.listdir(pth + '/' + fld):
                df = read_files(pth + '/' + fld + '/' + fl)
                if fld == 'claims':
                    claims = concatenate_dataframes(claims, df)
                elif fld == 'pharmacies':
                    pharmacies = concatenate_dataframes(pharmacies, df)
                elif fld == 'reverts':
                    reverts = concatenate_dataframes(reverts, df)

    ### since records for pharmacies in 'pharmacies' dataset are needed - filtering 'claims' dataset for it
    claims = claims[claims['npi'].isin(pharmacies['npi'].unique())]

    ### same is with the 'reverts' for claims
    reverts = reverts[reverts['claim_id'].isin(claims['id'])]

    ### adding info on reverts to 'claims' dataset
    claims['if_reverted'] = claims['id'].isin(reverts['claim_id']).astype(int)
    ### calculating the total_price (NB! excluding reverts)
    claims['total_price'] = claims['price'] * claims['quantity'] * abs(claims['if_reverted'] - 1)

    ### unit tests for checking the reverts and its count
    #print(claims.groupby(['if_reverted']).count())
    #print(reverts)

    ### assigment 2
    claims_processed = claims.groupby(['npi','ndc']).agg(
       fills=('id', 'count'),
       reverted=('if_reverted', 'sum'),
       avg_price=('price', 'mean'),
       total_price=('total_price', 'sum')
    ).reset_index()


    print_json(claims_processed.to_json(orient='records'),'assignment_2',pth_output)

    ### assigment 3
    claims_processed_2 = pd.merge(claims, pharmacies, on='npi', how='left')
    
    claims_processed_2 = claims_processed_2.groupby(['ndc','chain']).agg(
       avg_price=('price', 'mean')
    ).reset_index()
    
    result = claims_processed_2.groupby('ndc').apply(lambda x: x[['chain', 'avg_price']].to_dict(orient='records')).reset_index(name='chain')
    print_json(result.rename(columns={'chain': 'name'}).to_json(orient='records'),'assignment_3',pth_output)
    
    ## assigment 4
    claims_processed_3 = claims.groupby('ndc')['quantity'].apply(lambda x: x.mode().tolist()).reset_index()
    print_json(claims_processed_3.rename(columns={'quantity': 'most_prescribed_quantity'}).to_json(orient='records'),'assignment_4',pth_output)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing the app with the data from files')
    parser.add_argument('-path', dest='path', required=False,
                        help="path_to_entrypoint_folder")

    run(parser.parse_args())