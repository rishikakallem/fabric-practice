[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sales csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "sale_id", DestinationColumnName = "sale_id"], [SourceColumnName = "customer_id", DestinationColumnName = "customer_id"], [SourceColumnName = "product_id", DestinationColumnName = "product_id"], [SourceColumnName = "quantity", DestinationColumnName = "quantity"], [SourceColumnName = "sale_date", DestinationColumnName = "sale_date"], [SourceColumnName = "total_amount", DestinationColumnName = "total_amount"], [SourceColumnName = "last_modified", DestinationColumnName = "last_modified"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"sales csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "20706a0b-f5cf-407c-8c0c-39a3c7e901d9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "55426c38-4479-4246-a001-878ef1830557"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "sales.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 3", [Delimiter = ",", Columns = 7, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Trimmed text" = Table.TransformColumns(#"Promoted headers", {{"sale_id", each Text.Trim(_), type nullable text}, {"customer_id", each Text.Trim(_), type nullable text}, {"product_id", each Text.Trim(_), type nullable text}}),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Trimmed text", {"sale_id", "customer_id", "product_id"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed errors", "one", "1", Replacer.ReplaceText, {"quantity"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value", {{"quantity", Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each [quantity] > -2),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each [quantity] >= 0),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Filtered rows 1", {{"sale_date", type date}}),
  #"Replaced errors" = Table.ReplaceErrorValues(#"Changed column type 1", {{"sale_date", null}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Replaced errors", {{"total_amount", type number}}),
  #"Replaced errors 1" = Table.ReplaceErrorValues(#"Changed column type 2", {{"total_amount", null}}),
  #"Filtered rows 2" = Table.SelectRows(#"Replaced errors 1", each [total_amount] >= 0),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Filtered rows 2", {{"last_modified", type datetime}}),
  #"Replaced errors 2" = Table.ReplaceErrorValues(#"Changed column type 3", {{"last_modified", null}})
in
  #"Replaced errors 2";
shared #"sales csv_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "20706a0b-f5cf-407c-8c0c-39a3c7e901d9"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "55426c38-4479-4246-a001-878ef1830557"]}[Data],
  TableNavigation = Navigation_2{[Id = "silver_sales", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
