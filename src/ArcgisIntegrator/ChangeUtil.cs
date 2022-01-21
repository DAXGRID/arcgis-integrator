using ArcgisIntegrator.Config;
using System;

namespace ArcgisIntegrator;

internal static class ChangeUtil
{
    public static DataEvent MapChangeEvent(ArcgisChangeSet changeSet, TableWatch tableWatch)
    {
        var operation = GetOperation(changeSet);

        DataEvent? dataEvent = null;
        if (operation == Operation.Delete && changeSet.Delete is not null)
            dataEvent = new DataEvent(tableWatch, changeSet.Delete.Fields, operation);
        else if (operation != Operation.Delete && changeSet.Add is not null)
            dataEvent = new DataEvent(tableWatch, changeSet.Add.Fields, operation);

        return dataEvent ??
            throw new ArgumentException("Both Add and Delete cannot be null at the same time.", nameof(changeSet));
    }

    private static Operation GetOperation(ArcgisChangeSet changeSet)
    {
        var isAddOrUpdate = changeSet.Add is not null;
        var isDelete = changeSet.Delete is not null;
        if (isAddOrUpdate)
            return changeSet.Delete is null ? Operation.Create : Operation.Update;
        else if (isDelete)
            return Operation.Delete;

        throw new ArgumentException($"Could not convert to {nameof(Operation)}", nameof(changeSet));
    }
}
