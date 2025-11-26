import unittest
from datasurface.md import Ecosystem, ValidationTree, DataPlatform, EcosystemPipelineGraph, PlatformPipelineGraph
from typing import Any, Optional
from datasurface.md.model_loader import loadEcosystemFromEcoModule


class TestEcosystem(unittest.TestCase):
    def test_createEcosystem(self):
        ecosys: Optional[Ecosystem]
        ecoTree: Optional[ValidationTree]
        ecosys, ecoTree = loadEcosystemFromEcoModule(".")  # Check like a PR would first.
        self.assertIsNotNone(ecosys)
        self.assertIsNotNone(ecoTree)
        assert ecoTree is not None
        assert ecosys is not None
        if ecoTree.hasErrors():
            print("Ecosystem validation failed with errors:")
            ecoTree.printTree()
            raise Exception("Ecosystem validation failed")
        else:
            print("Ecosystem validated OK")
            if ecoTree.hasWarnings():
                print("Note: There are some warnings:")
                ecoTree.printTree()

        ecosys, ecoTree = loadEcosystemFromEcoModule(".", "prod")  # prod is the runtime environment name
        self.assertIsNotNone(ecosys)
        self.assertIsNotNone(ecoTree)
        assert ecoTree is not None
        assert ecosys is not None
        if ecoTree.hasErrors():
            print("Ecosystem validation failed with errors:")
            ecoTree.printTree()
            raise Exception("Ecosystem validation failed")
        else:
            print("Ecosystem validated OK")
            if ecoTree.hasWarnings():
                print("Note: There are some warnings:")
                ecoTree.printTree()
        vTree: ValidationTree = ecosys.lintAndHydrateCaches()
        if (vTree.hasErrors()):
            print("Ecosystem validation failed with errors:")
            vTree.printTree()
            raise Exception("Ecosystem validation failed")
        else:
            print("Ecosystem validated OK")
            if vTree.hasWarnings():
                print("Note: There are some warnings:")
                vTree.printTree()
        live_dp: DataPlatform[Any] = ecosys.getDataPlatformOrThrow("SCD1")  # type: ignore
        self.assertIsNotNone(live_dp)
        forensic_dp: DataPlatform[Any] = ecosys.getDataPlatformOrThrow("SCD2")  # type: ignore
        self.assertIsNotNone(forensic_dp)
        graph: EcosystemPipelineGraph = ecosys.getGraph()
        self.assertIsNotNone(graph)
        live_root: Optional[PlatformPipelineGraph] = graph.roots.get(live_dp.name)
        self.assertIsNotNone(live_root)
        forensic_root: Optional[PlatformPipelineGraph] = graph.roots.get(forensic_dp.name)
        self.assertIsNotNone(forensic_root)


if __name__ == "__main__":
    unittest.main()
