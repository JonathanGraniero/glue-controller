	arn := ackv1alpha1.AWSResourceName(databaseARN(ko))
	ko.Status.ACKResourceMetadata.ARN = &arn
