	if len(a.ko.Spec.PartitionIndexes) != len(b.ko.Spec.PartitionIndexes) {
		delta.Add("Spec.PartitionIndexes", a.ko.Spec.PartitionIndexes, b.ko.Spec.PartitionIndexes)
	} else if len(a.ko.Spec.PartitionIndexes) > 0 {
		if !partitionIndexesMatch(a.ko.Spec.PartitionIndexes, b.ko.Spec.PartitionIndexes) {
			delta.Add("Spec.PartitionIndexes", a.ko.Spec.PartitionIndexes, b.ko.Spec.PartitionIndexes)
		}
	}
