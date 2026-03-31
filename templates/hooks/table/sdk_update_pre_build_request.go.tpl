	if delta.DifferentAt("Spec.PartitionIndexes") && !delta.DifferentExcept("Spec.PartitionIndexes") {
		return desired, nil
	}
