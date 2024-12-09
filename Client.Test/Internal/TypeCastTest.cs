using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Types;
using InfluxDB3.Client.Internal;

namespace InfluxDB3.Client.Test.Internal;

public class TypeCastTest
{
    [Test]
    public void IsNumber()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TypeCasting.IsNumber(1), Is.True);
            Assert.That(TypeCasting.IsNumber(1.2), Is.True);
            Assert.That(TypeCasting.IsNumber(-1.2), Is.True);
        });

        Assert.Multiple(() =>
        {
            Assert.That(TypeCasting.IsNumber('1'), Is.False);
            Assert.That(TypeCasting.IsNumber('a'), Is.False);
            Assert.That(TypeCasting.IsNumber(true), Is.False);
            Assert.That(TypeCasting.IsNumber(null), Is.False);
        });
    }

    [Test]
    public void GetMappedValue()
    {
        // If pass the correct value type to GetMappedValue() it will return the value with a correct type
        // If pass the incorrect value type to GetMappedValue() it will NOT throws any error but return the passed value
        const string fieldName = "test";

        var field = GenerateIntField(fieldName);
        Assert.Multiple(() =>
        {
            Assert.That(TypeCasting.GetMappedValue(field, 1)!, Is.EqualTo(1));
            Assert.That(TypeCasting.GetMappedValue(field, "a")!, Is.EqualTo("a"));
        });

        field = GenerateUIntField(fieldName);
        Assert.Multiple(() =>
        {
            Assert.That(TypeCasting.GetMappedValue(field, 1)!, Is.EqualTo(1));
            Assert.That(TypeCasting.GetMappedValue(field, -1)!, Is.EqualTo(-1));
            Assert.That(TypeCasting.GetMappedValue(field, "a")!, Is.EqualTo("a"));
        });

        field = GenerateDoubleField(fieldName);
        Assert.Multiple(() =>
        {
            Assert.That(TypeCasting.GetMappedValue(field, 1.2)!, Is.EqualTo(1.2));
            Assert.That(TypeCasting.GetMappedValue(field, "a")!, Is.EqualTo("a"));
        });

        field = GenerateBooleanField(fieldName);
        Assert.Multiple(() =>
        {
            Assert.That(TypeCasting.GetMappedValue(field, true)!, Is.EqualTo(true));
            Assert.That(TypeCasting.GetMappedValue(field, 10)!, Is.EqualTo(10));
        });

        field = GenerateStringField(fieldName);
        Assert.Multiple(() =>
        {
            Assert.That(TypeCasting.GetMappedValue(field, "a")!, Is.EqualTo("a"));
            Assert.That(TypeCasting.GetMappedValue(field, 10)!, Is.EqualTo(10));
        });
        
        
        field = GenerateIntFieldTestTypeMeta(fieldName);
        Assert.That(TypeCasting.GetMappedValue(field, 1)!, Is.EqualTo(1));
    }

    private static Field GenerateIntFieldTestTypeMeta(string fieldName)
    {
        var meta = new Dictionary<string, string>
        {
            {
                "iox::column::type", "iox::column_type::field::test"
            }
        };
        return new Field(fieldName, Int64Type.Default, true, meta);
    }
    
    private static Field GenerateIntField(string fieldName)
    {
        var meta = new Dictionary<string, string>
        {
            {
                "iox::column::type", "iox::column_type::field::integer"
            }
        };
        return new Field(fieldName, Int64Type.Default, true, meta);
    }

    private static Field GenerateUIntField(string fieldName)
    {
        var meta = new Dictionary<string, string>
        {
            {
                "iox::column::type", "iox::column_type::field::uinteger"
            }
        };
        return new Field(fieldName, UInt64Type.Default, true, meta);
    }

    private static Field GenerateDoubleField(string fieldName)
    {
        var meta = new Dictionary<string, string>
        {
            {
                "iox::column::type", "iox::column_type::field::float"
            }
        };
        return new Field(fieldName, DoubleType.Default, true, meta);
    }

    private static Field GenerateBooleanField(string fieldName)
    {
        var meta = new Dictionary<string, string>
        {
            {
                "iox::column::type", "iox::column_type::field::boolean"
            }
        };
        return new Field(fieldName, BooleanType.Default, true, meta);
    }

    private static Field GenerateStringField(string fieldName)
    {
        var meta = new Dictionary<string, string>
        {
            {
                "iox::column::type", "iox::column_type::field::string"
            }
        };
        return new Field(fieldName, StringType.Default, true, meta);
    }
}