    tableInput, err := rm.buildTableInput(desired)
    if err != nil {
        return nil, err
    }
    input.TableInput = tableInput
