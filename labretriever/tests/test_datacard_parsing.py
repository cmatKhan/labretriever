"""Test script to verify datacard parsing with new environmental_conditions."""

import yaml  # type: ignore

from labretriever.models import DatasetCard
from labretriever.tests.example_datacards import (
    EXAMPLE_1_SIMPLE_TOPLEVEL,
    EXAMPLE_2_COMPLEX_FIELD_DEFINITIONS,
    EXAMPLE_3_PARTITIONED_WITH_METADATA,
)


def test_example_1():
    """Test parsing example 1: simple top-level conditions."""
    print("=" * 80)
    print("Testing Example 1: Simple Top-Level Conditions")
    print("=" * 80)

    # Extract YAML from markdown
    yaml_content = EXAMPLE_1_SIMPLE_TOPLEVEL.split("---")[1]
    data = yaml.safe_load(yaml_content)

    try:
        card = DatasetCard(**data)
        print("✓ Successfully parsed Example 1")
        print(f"  - Configs: {len(card.configs)}")
        print(
            "  - Top-level experimental_conditions: "
            f"{card.experimental_conditions is not None}"
        )

        if card.experimental_conditions:
            env_cond = card.experimental_conditions.environmental_conditions
            if env_cond:
                print(f"  - Temperature: {env_cond.temperature_celsius}°C")
                print(f"  - Cultivation: {env_cond.cultivation_method}")
                if env_cond.media:
                    print(f"  - Media: {env_cond.media.name}")
                    print(f"    - Carbon sources: {len(env_cond.media.carbon_source)}")
                    print(
                        f"    - Nitrogen sources: {len(env_cond.media.nitrogen_source)}"
                    )

        # Check field-level definitions
        config = card.configs[0]
        for feature in config.dataset_info.features:
            if feature.definitions:
                print(
                    f"  - Feature '{feature.name}' has "
                    f"{len(feature.definitions)} definitions"
                )
                for def_name in feature.definitions.keys():
                    print(f"    - {def_name}")

        print()
        return True
    except Exception as e:
        print(f"✗ Failed to parse Example 1: {e}")
        import traceback

        traceback.print_exc()
        print()
        return False


def test_example_2():
    """Test parsing example 2: complex field-level definitions."""
    print("=" * 80)
    print("Testing Example 2: Complex Field-Level Definitions")
    print("=" * 80)

    yaml_content = EXAMPLE_2_COMPLEX_FIELD_DEFINITIONS.split("---")[1]
    data = yaml.safe_load(yaml_content)

    try:
        card = DatasetCard(**data)
        print("✓ Successfully parsed Example 2")
        print(f"  - Configs: {len(card.configs)}")
        print(f"  - Strain information: {card.strain_information is not None}")

        # Check field-level definitions
        config = card.configs[0]
        for feature in config.dataset_info.features:
            if feature.definitions:
                print(
                    f"  - Feature '{feature.name}' has "
                    f"{len(feature.definitions)} definitions:"
                )
                for def_name, def_value in feature.definitions.items():
                    print(f"    - {def_name}")
                    if "environmental_conditions" in def_value:
                        env = def_value["environmental_conditions"]
                        if "temperature_celsius" in env:
                            print(f"      Temperature: {env['temperature_celsius']}°C")
                        if "media" in env:
                            print(f"      Media: {env['media']['name']}")

        print()
        return True
    except Exception as e:
        print(f"✗ Failed to parse Example 2: {e}")
        import traceback

        traceback.print_exc()
        print()
        return False


def test_example_3():
    """Test parsing example 3: partitioned with metadata."""
    print("=" * 80)
    print("Testing Example 3: Partitioned with Metadata")
    print("=" * 80)

    yaml_content = EXAMPLE_3_PARTITIONED_WITH_METADATA.split("---")[1]
    data = yaml.safe_load(yaml_content)

    try:
        card = DatasetCard(**data)
        print("✓ Successfully parsed Example 3")
        print(f"  - Configs: {len(card.configs)}")
        print(
            "  - Top-level experimental_conditions: "
            f"{card.experimental_conditions is not None}"
        )

        if card.experimental_conditions:
            env_cond = card.experimental_conditions.environmental_conditions
            if env_cond and env_cond.media:
                print(f"  - Top-level media: {env_cond.media.name}")

        # Check config-level experimental_conditions
        for config in card.configs:
            if config.experimental_conditions:
                print(f"  - Config '{config.config_name}' has experimental_conditions")
                env_cond = config.experimental_conditions.environmental_conditions
                if env_cond and env_cond.media:
                    print(f"    - Media: {env_cond.media.name}")
                    print(f"    - Temperature: {env_cond.temperature_celsius}°C")

        print()
        return True
    except Exception as e:
        print(f"✗ Failed to parse Example 3: {e}")
        import traceback

        traceback.print_exc()
        print()
        return False


if __name__ == "__main__":
    results = []

    results.append(test_example_1())
    results.append(test_example_2())
    results.append(test_example_3())

    print("=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Passed: {sum(results)}/{len(results)}")

    if all(results):
        print("\n✓ All tests passed!")
        exit(0)
    else:
        print("\n✗ Some tests failed")
        exit(1)
