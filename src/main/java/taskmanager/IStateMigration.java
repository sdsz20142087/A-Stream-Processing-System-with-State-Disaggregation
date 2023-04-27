package taskmanager;

import pb.Tm;

import java.util.List;

public interface IStateMigration {
    public void handleStageMigration(List<Tm.OperatorConfig> newConfigs);
}
