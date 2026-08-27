# DELETE 产生墓碑，回滚可重建

删除 = 移除当前 Config Entry + 向 History 追加一条不含内容的 `op=delete` 墓碑；Rollback 到墓碑之前的任意版本即可重建条目。History 由此成为完整审计线索，"误删可救"覆盖了配置中心的高频事故。

硬删除（连 History 一并清除）被否决——审计完整性与可救性优先。
