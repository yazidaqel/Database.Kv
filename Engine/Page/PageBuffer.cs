using System;


namespace FraudDetector.Database.Kv.Engine.Page
{
    public class PageBuffer
    {
        public string Key { get; set; }
        public DateTimeOffset AppendedAt { get; set; }
        public byte[] Value { get; set; }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;

            if (!(obj is PageBuffer))
                return false;

            PageBuffer pageBuffer = (PageBuffer)obj;

            return pageBuffer.Key == Key && pageBuffer.AppendedAt == AppendedAt;
        }

        public override int GetHashCode()
        {
            return Key.GetHashCode() + AppendedAt.GetHashCode();
        }
    }
}
