
## Deploying
`func azure functionapp publish AzureDoFinDetection`

## Use in Azure Data Factory

**Function Name**:

```
@concat(
'process_file?',
'&container=', item().container,
'&path=', item().name,
'&id_field=', pipeline().parameters.id_field,
'&folder_id_idx=', pipeline().parameters.folder_id_idx,
'&con_env_in=', pipeline().parameters.con_env_in,
'&con_env_out=', pipeline().parameters.con_env_out,
'&folder_out=', pipeline().parameters.folder_out,
'&container_out=', pipeline().parameters.container_out,
'&only_single=', pipeline().parameters.only_single
)
```
**Function Key**: 
- Authentication: Anonymous
- Function Key: use default key from Function App Service