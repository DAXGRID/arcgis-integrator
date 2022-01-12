using System;
using System.Collections.Generic;

namespace KonstantDataValidator.Change;

public static class ChangeUtil
{
    public static ChangeEvent MapChangeEvent(ChangeSet changeSet, TableWatch tableWatch)
    {
        var operation = GetOperation(changeSet);

        IReadOnlyDictionary<string, object> fields;
        if (operation == Operation.Delete && changeSet.Delete is not null)
            fields = changeSet.Delete.Fields;
        else if (operation != Operation.Delete && changeSet.Add is not null)
            fields = changeSet.Add.Fields;
        else
            throw new ArgumentException("Both Add and Delete in changeset cannot be null at the same time");

        return new ChangeEvent(tableWatch, fields, operation);
    }

    private static Operation GetOperation(ChangeSet changeSet)
    {
        if (changeSet.Add is not null && changeSet.Delete is null)
            return Operation.Create;
        else if (changeSet.Add is not null && changeSet.Delete is not null)
            return Operation.Update;
        else if (changeSet.Add is null && changeSet.Delete is not null)
            return Operation.Delete;
        else
            throw new Exception($"Could not convert {nameof(ChangeSet)} to {nameof(Operation)}");
    }
}
