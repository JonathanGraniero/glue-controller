{{ $CRD := .CRD }}
{{ $SDKAPI := .SDKAPI }}
{{/* Generate helper methods for Database */}}
{{- $operation := (index $SDKAPI.API.Operations "UpdateDatabase" ) -}}
{{- range $dbFieldName, $dbFieldMemberRef := $operation.InputRef.Shape.MemberRefs -}}
{{- if eq $dbFieldName "DatabaseInput" }}
func (rm *resourceManager) new{{ $dbFieldName }}(
	    r *resource,
        res *svcsdk.{{ $operation.InputRef.ShapeName }},
) (interface{}, error) {
    databaseInput := &svcsdktypes.{{ $dbFieldName }}{}
{{ GoCodeSetSDKForStruct $CRD "" "databaseInput" $dbFieldMemberRef "" "r.ko.Spec.DatabaseInput" 1 }}
    res.DatabaseInput = databaseInput
	return nil, nil
}
{{- end }}
{{- end }}

