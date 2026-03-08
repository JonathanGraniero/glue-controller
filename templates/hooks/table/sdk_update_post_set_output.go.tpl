	if err = rm.syncPartitionIndexes(ctx, desired, latest); err != nil {
		return nil, err
	}
