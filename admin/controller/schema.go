package controller

func tansferSchemaName(schemaName string) string {
	if schemaName == "AllDataBases" {
		return "*"
	}
	return schemaName
}

func tansferTableName(tableName string) string {
	if tableName == "AllTables" {
		return "*"
	}
	return tableName
}
