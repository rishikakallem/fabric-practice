[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "customers csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "customer_id", DestinationColumnName = "customer_id"], [SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "city", DestinationColumnName = "city"], [SourceColumnName = "created_date", DestinationColumnName = "created_date"], [SourceColumnName = "last_modified", DestinationColumnName = "last_modified"], [SourceColumnName = "gender_clean", DestinationColumnName = "gender_clean"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"customers csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "20706a0b-f5cf-407c-8c0c-39a3c7e901d9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "55426c38-4479-4246-a001-878ef1830557"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "customers.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 3", [Delimiter = ",", Columns = 6, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"customer_id", type text}, {"name", type text}, {"gender", type text}, {"city", type text}, {"created_date", type text}, {"last_modified", type datetime}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type", {"customer_id"}),
  #"Filtered rows" = Table.SelectRows(#"Removed duplicates", each ([customer_id] <> "")),
  #"Trimmed text" = Table.TransformColumns(#"Filtered rows", {{"city", each Text.Trim(_), type nullable text}, {"gender", each Text.Trim(_), type nullable text}, {"name", each Text.Trim(_), type nullable text}}),
  #"Inserted conditional column" = Table.AddColumn(#"Trimmed text", "gender_clean", each if [gender] = "Female" then "F" else if [gender] = "F" then "F" else if [gender] = "Male" then "M" else if [gender] = "M" then "M" else "Unknown"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted conditional column", {{"created_date", type date}}),
  #"Replaced errors" = Table.ReplaceErrorValues(#"Changed column type 1", {{"created_date", null}}),
  #"Removed columns" = Table.RemoveColumns(#"Replaced errors", {"gender"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed columns", {{"gender_clean", type text}})
in
  #"Changed column type 2";
shared #"customers csv_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "20706a0b-f5cf-407c-8c0c-39a3c7e901d9"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "55426c38-4479-4246-a001-878ef1830557"]}[Data],
  TableNavigation = Navigation_2{[Id = "silver_customers", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
