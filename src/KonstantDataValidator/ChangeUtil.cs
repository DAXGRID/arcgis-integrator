using System;

namespace KonstantDataValidator;

public static class ChangeUtil
{
    public static Operation GetOperation(ChangeSet changeSet)
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
