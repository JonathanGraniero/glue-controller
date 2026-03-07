// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package table

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/glue/types"

	svcapitypes "github.com/aws-controllers-k8s/glue-controller/apis/v1alpha1"
)

// buildTableInput constructs a TableInput struct from the resource spec,
// used for both CreateTable and UpdateTable API calls.
func (rm *resourceManager) buildTableInput(
	r *resource,
) (*svcsdktypes.TableInput, error) {
	tableInput := &svcsdktypes.TableInput{}
	if r.ko.Spec.Name != nil {
		tableInput.Name = r.ko.Spec.Name
	}
	if r.ko.Spec.Description != nil {
		tableInput.Description = r.ko.Spec.Description
	}
	if r.ko.Spec.Owner != nil {
		tableInput.Owner = r.ko.Spec.Owner
	}
	if r.ko.Spec.Parameters != nil {
		tableInput.Parameters = aws.ToStringMap(r.ko.Spec.Parameters)
	}
	if r.ko.Spec.PartitionKeys != nil {
		cols := []svcsdktypes.Column{}
		for _, col := range r.ko.Spec.PartitionKeys {
			if col == nil {
				continue
			}
			sdkCol := svcsdktypes.Column{}
			if col.Name != nil {
				sdkCol.Name = col.Name
			}
			if col.Comment != nil {
				sdkCol.Comment = col.Comment
			}
			if col.Type != nil {
				sdkCol.Type = col.Type
			}
			if col.Parameters != nil {
				sdkCol.Parameters = aws.ToStringMap(col.Parameters)
			}
			cols = append(cols, sdkCol)
		}
		tableInput.PartitionKeys = cols
	}
	if r.ko.Spec.Retention != nil {
		tableInput.Retention = int32(*r.ko.Spec.Retention)
	}
	if r.ko.Spec.StorageDescriptor != nil {
		sd, err := rm.buildStorageDescriptor(r.ko.Spec.StorageDescriptor)
		if err != nil {
			return nil, err
		}
		tableInput.StorageDescriptor = sd
	}
	if r.ko.Spec.TableType != nil {
		tableInput.TableType = r.ko.Spec.TableType
	}
	if r.ko.Spec.TargetTable != nil {
		tableInput.TargetTable = &svcsdktypes.TableIdentifier{
			CatalogId:    r.ko.Spec.TargetTable.CatalogID,
			DatabaseName: r.ko.Spec.TargetTable.DatabaseName,
			Name:         r.ko.Spec.TargetTable.Name,
			Region:       r.ko.Spec.TargetTable.Region,
		}
	}
	if r.ko.Spec.ViewExpandedText != nil {
		tableInput.ViewExpandedText = r.ko.Spec.ViewExpandedText
	}
	if r.ko.Spec.ViewOriginalText != nil {
		tableInput.ViewOriginalText = r.ko.Spec.ViewOriginalText
	}
	return tableInput, nil
}

// buildStorageDescriptor converts the k8s StorageDescriptor into the SDK type.
func (rm *resourceManager) buildStorageDescriptor(
	sd *svcapitypes.StorageDescriptor,
) (*svcsdktypes.StorageDescriptor, error) {
	result := &svcsdktypes.StorageDescriptor{}
	if sd.Location != nil {
		result.Location = sd.Location
	}
	if sd.InputFormat != nil {
		result.InputFormat = sd.InputFormat
	}
	if sd.OutputFormat != nil {
		result.OutputFormat = sd.OutputFormat
	}
	if sd.Compressed != nil {
		result.Compressed = *sd.Compressed
	}
	if sd.NumberOfBuckets != nil {
		result.NumberOfBuckets = int32(*sd.NumberOfBuckets)
	}
	if sd.StoredAsSubDirectories != nil {
		result.StoredAsSubDirectories = *sd.StoredAsSubDirectories
	}
	if sd.Parameters != nil {
		result.Parameters = aws.ToStringMap(sd.Parameters)
	}
	if sd.AdditionalLocations != nil {
		locs := []string{}
		for _, l := range sd.AdditionalLocations {
			if l != nil {
				locs = append(locs, *l)
			}
		}
		result.AdditionalLocations = locs
	}
	if sd.BucketColumns != nil {
		cols := []string{}
		for _, c := range sd.BucketColumns {
			if c != nil {
				cols = append(cols, *c)
			}
		}
		result.BucketColumns = cols
	}
	if sd.Columns != nil {
		cols := []svcsdktypes.Column{}
		for _, col := range sd.Columns {
			if col == nil {
				continue
			}
			sdkCol := svcsdktypes.Column{}
			if col.Name != nil {
				sdkCol.Name = col.Name
			}
			if col.Comment != nil {
				sdkCol.Comment = col.Comment
			}
			if col.Type != nil {
				sdkCol.Type = col.Type
			}
			if col.Parameters != nil {
				sdkCol.Parameters = aws.ToStringMap(col.Parameters)
			}
			cols = append(cols, sdkCol)
		}
		result.Columns = cols
	}
	if sd.SortColumns != nil {
		orders := []svcsdktypes.Order{}
		for _, o := range sd.SortColumns {
			if o == nil {
				continue
			}
			sdkOrder := svcsdktypes.Order{}
			if o.Column != nil {
				sdkOrder.Column = o.Column
			}
			if o.SortOrder != nil {
				sdkOrder.SortOrder = int32(*o.SortOrder)
			}
			orders = append(orders, sdkOrder)
		}
		result.SortColumns = orders
	}
	if sd.SerdeInfo != nil {
		result.SerdeInfo = &svcsdktypes.SerDeInfo{
			Name:                 sd.SerdeInfo.Name,
			SerializationLibrary: sd.SerdeInfo.SerializationLibrary,
		}
		if sd.SerdeInfo.Parameters != nil {
			result.SerdeInfo.Parameters = aws.ToStringMap(sd.SerdeInfo.Parameters)
		}
	}
	if sd.SkewedInfo != nil {
		result.SkewedInfo = &svcsdktypes.SkewedInfo{}
		if sd.SkewedInfo.SkewedColumnNames != nil {
			names := []string{}
			for _, n := range sd.SkewedInfo.SkewedColumnNames {
				if n != nil {
					names = append(names, *n)
				}
			}
			result.SkewedInfo.SkewedColumnNames = names
		}
		if sd.SkewedInfo.SkewedColumnValues != nil {
			vals := []string{}
			for _, v := range sd.SkewedInfo.SkewedColumnValues {
				if v != nil {
					vals = append(vals, *v)
				}
			}
			result.SkewedInfo.SkewedColumnValues = vals
		}
		if sd.SkewedInfo.SkewedColumnValueLocationMaps != nil {
			result.SkewedInfo.SkewedColumnValueLocationMaps = aws.ToStringMap(sd.SkewedInfo.SkewedColumnValueLocationMaps)
		}
	}
	if sd.SchemaReference != nil {
		result.SchemaReference = &svcsdktypes.SchemaReference{
			SchemaVersionId:     sd.SchemaReference.SchemaVersionID,
			SchemaVersionNumber: sd.SchemaReference.SchemaVersionNumber,
		}
		if sd.SchemaReference.SchemaID != nil {
			result.SchemaReference.SchemaId = &svcsdktypes.SchemaId{
				RegistryName: sd.SchemaReference.SchemaID.RegistryName,
				SchemaArn:    sd.SchemaReference.SchemaID.SchemaARN,
				SchemaName:   sd.SchemaReference.SchemaID.SchemaName,
			}
		}
	}
	return result, nil
}

// setStorageDescriptor maps an SDK StorageDescriptor response into the k8s type.
func (rm *resourceManager) setStorageDescriptor(
	sd *svcsdktypes.StorageDescriptor,
) *svcapitypes.StorageDescriptor {
	result := &svcapitypes.StorageDescriptor{}
	if sd.Location != nil {
		result.Location = sd.Location
	}
	if sd.InputFormat != nil {
		result.InputFormat = sd.InputFormat
	}
	if sd.OutputFormat != nil {
		result.OutputFormat = sd.OutputFormat
	}
	if sd.Compressed {
		result.Compressed = &sd.Compressed
	}
	if sd.NumberOfBuckets != 0 {
		nb := int64(sd.NumberOfBuckets)
		result.NumberOfBuckets = &nb
	}
	if sd.StoredAsSubDirectories {
		result.StoredAsSubDirectories = &sd.StoredAsSubDirectories
	}
	if sd.Parameters != nil {
		result.Parameters = aws.StringMap(sd.Parameters)
	}
	if sd.AdditionalLocations != nil {
		locs := []*string{}
		for i := range sd.AdditionalLocations {
			locs = append(locs, &sd.AdditionalLocations[i])
		}
		result.AdditionalLocations = locs
	}
	if sd.BucketColumns != nil {
		cols := []*string{}
		for i := range sd.BucketColumns {
			cols = append(cols, &sd.BucketColumns[i])
		}
		result.BucketColumns = cols
	}
	if sd.Columns != nil {
		cols := []*svcapitypes.Column{}
		for _, col := range sd.Columns {
			c := &svcapitypes.Column{
				Name:    col.Name,
				Comment: col.Comment,
				Type:    col.Type,
			}
			if col.Parameters != nil {
				c.Parameters = aws.StringMap(col.Parameters)
			}
			cols = append(cols, c)
		}
		result.Columns = cols
	}
	if sd.SortColumns != nil {
		orders := []*svcapitypes.Order{}
		for _, o := range sd.SortColumns {
			so := int64(o.SortOrder)
			orders = append(orders, &svcapitypes.Order{
				Column:    o.Column,
				SortOrder: &so,
			})
		}
		result.SortColumns = orders
	}
	if sd.SerdeInfo != nil {
		result.SerdeInfo = &svcapitypes.SerDeInfo{
			Name:                 sd.SerdeInfo.Name,
			SerializationLibrary: sd.SerdeInfo.SerializationLibrary,
		}
		if sd.SerdeInfo.Parameters != nil {
			result.SerdeInfo.Parameters = aws.StringMap(sd.SerdeInfo.Parameters)
		}
	}
	if sd.SkewedInfo != nil {
		result.SkewedInfo = &svcapitypes.SkewedInfo{}
		if sd.SkewedInfo.SkewedColumnNames != nil {
			names := []*string{}
			for i := range sd.SkewedInfo.SkewedColumnNames {
				names = append(names, &sd.SkewedInfo.SkewedColumnNames[i])
			}
			result.SkewedInfo.SkewedColumnNames = names
		}
		if sd.SkewedInfo.SkewedColumnValues != nil {
			vals := []*string{}
			for i := range sd.SkewedInfo.SkewedColumnValues {
				vals = append(vals, &sd.SkewedInfo.SkewedColumnValues[i])
			}
			result.SkewedInfo.SkewedColumnValues = vals
		}
		if sd.SkewedInfo.SkewedColumnValueLocationMaps != nil {
			result.SkewedInfo.SkewedColumnValueLocationMaps = aws.StringMap(sd.SkewedInfo.SkewedColumnValueLocationMaps)
		}
	}
	if sd.SchemaReference != nil {
		result.SchemaReference = &svcapitypes.SchemaReference{
			SchemaVersionID:     sd.SchemaReference.SchemaVersionId,
			SchemaVersionNumber: sd.SchemaReference.SchemaVersionNumber,
		}
		if sd.SchemaReference.SchemaId != nil {
			result.SchemaReference.SchemaID = &svcapitypes.SchemaID{
				RegistryName: sd.SchemaReference.SchemaId.RegistryName,
				SchemaARN:    sd.SchemaReference.SchemaId.SchemaArn,
				SchemaName:   sd.SchemaReference.SchemaId.SchemaName,
			}
		}
	}
	return result
}

// tableARN returns the ARN of the Glue table.
func tableARN(table *svcapitypes.Table) string {
	if table.Status.ACKResourceMetadata == nil ||
		table.Status.ACKResourceMetadata.Region == nil ||
		table.Status.ACKResourceMetadata.OwnerAccountID == nil ||
		table.Spec.DatabaseName == nil ||
		table.Spec.Name == nil {
		return ""
	}
	return fmt.Sprintf("arn:aws:glue:%s:%s:table/%s/%s",
		*table.Status.ACKResourceMetadata.Region,
		*table.Status.ACKResourceMetadata.OwnerAccountID,
		*table.Spec.DatabaseName,
		*table.Spec.Name,
	)
}
